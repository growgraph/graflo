"""Equivalence clusters over compose-time vertex/relation mappings.

A :class:`~graflo.architecture.evolution.ops.VertexEquivalence` (or
:class:`~graflo.architecture.evolution.ops.RelationEquivalence`) declares one
n-ary cluster directly: ``left`` / ``right`` name one or more members on each
side, collapsing onto one composed name. :func:`index_clusters` is the
consistency check over the *declared* clusters of one
:class:`~graflo.architecture.evolution.ops.ComposeManifestsOp` — there is no
connected-component search left to do (one declaration *is* one cluster); it
validates that the declarations do not overlap or collapse into each other by
accident:

* no ``(side, name)`` may be claimed by two declarations — that is the
  author's job to state as one cluster, not two;
* two declarations must not share one composed name — sharing one collapses
  them into one composed class, which must be spelled as one n-ary cluster so
  it is visible to review, not left implicit;
* a composed name that already exists as a *different*, non-member class on a
  side must not be silently merged into — add it to the cluster explicitly.
  The one exception is a name that *another* declaration renames away: the
  lowered map applies in one step, so the side lands on a vacated name whether
  it is one member or a merge.

Members and composed names are resolved before indexing — ``into`` may be
omitted and a member may be spelled by its canonical name — by
:func:`~graflo.architecture.evolution.canonical.resolve_clusters`, which hands
the resolved shapes in as :class:`ClusterSpec`\\ s. Nodes are ``(side, name)``
pairs so a class named ``Org`` on the left is never confused with ``Org`` on
the right.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeVar

from graflo.architecture.schema.naming import canonical_slug

from .ops import ComposeManifestsOp, RelationEquivalence, VertexEquivalence

Side = Literal["left", "right"]

DeclarationT = TypeVar("DeclarationT", VertexEquivalence, RelationEquivalence)


class ClusterConflictError(ValueError):
    """Two or more equivalence declarations conflict over cluster membership."""


@dataclass(frozen=True)
class ClusterSpec:
    """One declaration's resolved shape: members in the manifests' own names, and its composed name.

    ``aliases`` records, per side, ``{declared spelling: resolved name}`` for
    members the author spelled by their canonical name — the declaration's own
    per-member maps (property equivalences, ``SideIdentity.members``) are keyed
    by the spelling the author used.
    """

    left: tuple[str, ...]
    right: tuple[str, ...]
    into: str
    aliases: dict[Side, dict[str, str]] = field(default_factory=dict)


@dataclass(frozen=True)
class Cluster(Generic[DeclarationT]):
    """One n-ary equivalence cluster, over vertices or relations, in resolved names."""

    left: tuple[str, ...]
    right: tuple[str, ...]
    into: str
    declaration: DeclarationT
    aliases: dict[Side, dict[str, str]] = field(default_factory=dict)

    def members(self, side: Side) -> tuple[str, ...]:
        return self.left if side == "left" else self.right

    def resolved(self, side: Side, declared: str) -> str:
        """The resolved member name for a spelling the declaration used."""
        return self.aliases.get(side, {}).get(declared, declared)

    def property_maps(self, side: Side) -> dict[str, dict[str, str]]:
        """The declaration's per-member attribute maps, keyed by resolved member name."""
        declaration = self.declaration
        if not isinstance(declaration, VertexEquivalence):
            return {}
        return {
            self.resolved(side, member): attrs
            for member, attrs in declaration.property_maps(side).items()
        }


RelationCluster = Cluster[RelationEquivalence]


@dataclass(frozen=True)
class ClusterIndex:
    """Every declared cluster of one compose op, validated for consistency."""

    vertices: tuple[Cluster[VertexEquivalence], ...]
    relations: tuple[Cluster[RelationEquivalence], ...]

    @property
    def labels(self) -> frozenset[str]:
        return frozenset(c.into for c in self.vertices)

    @property
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

    def cluster_for_label(self, into: str) -> Cluster[VertexEquivalence] | None:
        """The vertex cluster collapsing onto *into*, or ``None``."""
        return next((c for c in self.vertices if c.into == into), None)


def _check_declarations(
    specs: Sequence[ClusterSpec],
    *,
    kind: str,
    left_names: Collection[str],
    right_names: Collection[str],
) -> None:
    """Shared overlap / shared-name / occupied-name checks for one declaration kind."""
    claimed: dict[tuple[Side, str], int] = {}
    into_owner: dict[str, int] = {}
    claimed_by_side: dict[Side, set[str]] = {"left": set(), "right": set()}
    for spec in specs:
        claimed_by_side["left"].update(spec.left)
        claimed_by_side["right"].update(spec.right)

    for index, spec in enumerate(specs):
        into = spec.into
        for side, members in (("left", spec.left), ("right", spec.right)):
            for name in members:
                key: tuple[Side, str] = (side, name)  # type: ignore[assignment]
                prior = claimed.get(key)
                if prior is not None and prior != index:
                    raise ClusterConflictError(
                        f"{kind}: {side}:{name} is claimed by two equivalence "
                        f"declarations (into {specs[prior].into!r} and "
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
            ("left", spec.left, left_names),
            ("right", spec.right, right_names),
        ):
            if into not in names or into in members:
                continue
            if into in claimed_by_side[side]:
                # Another declaration renames the occupant away, and the
                # lowered map applies in one step, so this side lands on a
                # vacated name — for a single member and a merge alike.
                continue
            raise ClusterConflictError(
                f"{kind}: into {into!r} already exists on the {side} side "
                f"but is not a member of its cluster "
                f"({side}={list(members)}); add it to this cluster's `{side}` "
                "to merge into it, declare it in another cluster so it is "
                "renamed away, or pick a different `into`"
            )


def declared_spec(declaration: VertexEquivalence | RelationEquivalence) -> ClusterSpec:
    """The shape a declaration states outright, with no canonical map to consult."""
    if declaration.into is None:
        raise ValueError(
            f"equivalence {declaration.left_members} ~ "
            f"{declaration.right_members} has no `into`; a composed name comes "
            "from `into`, from a canonical map on the compose op, or from one "
            "spelling every member shares — resolve it through "
            "validate_and_complete_canonical_map, or name it"
        )
    return ClusterSpec(
        left=tuple(declaration.left_members),
        right=tuple(declaration.right_members),
        into=declaration.into,
    )


def index_clusters(
    op: ComposeManifestsOp,
    *,
    left_vertices: Collection[str] = (),
    right_vertices: Collection[str] = (),
    left_relations: Collection[str] = (),
    right_relations: Collection[str] = (),
    vertex_specs: Sequence[ClusterSpec] | None = None,
    relation_specs: Sequence[ClusterSpec] | None = None,
) -> ClusterIndex:
    """Validate and index the declared clusters of *op*.

    *vertex_specs* / *relation_specs* are the resolved shapes, aligned with the
    op's declaration lists; omitted, each declaration is taken as written
    (which requires ``into``). The name collections are what a composed name
    may collide with on each side.

    Raises :class:`ClusterConflictError` on an overlapping declaration, two
    declarations sharing one composed name, or a composed name that would
    silently occupy an existing non-member class on a side.
    """
    if vertex_specs is None:
        vertex_specs = [declared_spec(v) for v in op.vertex_equivalences]
    if relation_specs is None:
        relation_specs = [declared_spec(r) for r in op.relation_equivalences]
    _check_declarations(
        vertex_specs,
        kind="vertex equivalence",
        left_names=left_vertices,
        right_names=right_vertices,
    )
    _check_declarations(
        relation_specs,
        kind="relation equivalence",
        left_names=left_relations,
        right_names=right_relations,
    )
    return ClusterIndex(
        vertices=tuple(
            Cluster(
                left=spec.left,
                right=spec.right,
                into=spec.into,
                declaration=v,
                aliases=spec.aliases,
            )
            for v, spec in zip(op.vertex_equivalences, vertex_specs, strict=True)
        ),
        relations=tuple(
            Cluster(
                left=spec.left,
                right=spec.right,
                into=spec.into,
                declaration=r,
                aliases=spec.aliases,
            )
            for r, spec in zip(op.relation_equivalences, relation_specs, strict=True)
        ),
    )


def did_you_mean(name: str, candidates: Iterable[str]) -> str:
    """A suffix naming a candidate that denotes the same concept, if any.

    Authoring an equivalence in the wrong convention is the likeliest mistake
    at this boundary, and "not in left manifest" alone is a dead end when the
    vertex is right there under another spelling.
    """
    key = canonical_slug(name)
    near = sorted(c for c in candidates if c != name and canonical_slug(c) == key)
    if not near:
        return ""
    return (
        f"; it has {near[0]!r}, which denotes the same concept — author the "
        "equivalence in the manifest's own spelling"
    )


def check_member_existence(
    vertex_clusters: Iterable[ClusterSpec | Cluster[VertexEquivalence]],
    relation_clusters: Iterable[ClusterSpec | Cluster[RelationEquivalence]],
    *,
    left_vertex_names: Collection[str],
    right_vertex_names: Collection[str],
    left_relation_names: Collection[str],
    right_relation_names: Collection[str],
) -> None:
    """Every member must exist on its side; a near-miss spelling is named."""
    for cluster in vertex_clusters:
        for member in cluster.left:
            if member not in left_vertex_names:
                raise ValueError(
                    f"compose_manifests: left vertex {member!r} not in left "
                    f"manifest{did_you_mean(member, left_vertex_names)}"
                )
        for member in cluster.right:
            if member not in right_vertex_names:
                raise ValueError(
                    f"compose_manifests: right vertex {member!r} not in right "
                    f"manifest{did_you_mean(member, right_vertex_names)}"
                )
    for cluster in relation_clusters:
        for member in cluster.left:
            if member not in left_relation_names:
                raise ValueError(
                    f"compose_manifests: left relation {member!r} not in left manifest"
                )
        for member in cluster.right:
            if member not in right_relation_names:
                raise ValueError(
                    f"compose_manifests: right relation {member!r} not in "
                    "right manifest"
                )
