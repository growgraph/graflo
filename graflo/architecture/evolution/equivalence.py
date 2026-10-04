"""The resolved shape of a merge's groups.

A :class:`Cluster` is one group as merge applies it: its members on each side
in the manifests' own names, closed over every class a vocabulary merges with
one of them, and the one name the group takes. A :class:`ClusterIndex` holds
every group of one merge op. Both are built by
:mod:`~graflo.architecture.evolution.naming_graph`, which also owns every rule
about which groups may exist; this module only carries the result.

Nodes are ``(side, name)`` pairs, so a class named ``Org`` on the left is never
confused with ``Org`` on the right, and :func:`subject` spells them as the ids
refusals and previews point at.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeVar

from graflo.architecture.schema.naming import canonical_slug

from .ops import RelationEquivalence, VertexEquivalence

Side = Literal["left", "right"]

#: What a declaration is about: a class or a relation.
Kind = Literal["vertex", "relation"]

#: Where a name lives. The two sides' own vocabularies, plus the two the
#: merge declarations establish: a **merged** name is what a cluster
#: collapses onto, a **canonical** one what a declared map renames into
#: without any cluster naming it.
SubjectScope = Literal["left", "right", "merged", "canonical"]

DeclarationT = TypeVar("DeclarationT", VertexEquivalence, RelationEquivalence)


def subject(scope: SubjectScope, name: str, attr: str | None = None) -> str:
    """A stable id for the class or attribute a refusal is about.

    Every refusal at this boundary names the declarations it refuses, and a
    caller that wants to *point* at them — a preview, a diagram, an editor —
    needs those names as data rather than parsed back out of prose.

    Args:
        scope: Which vocabulary the name lives in.
        name: The class or relation name.
        attr: An attribute of it, when the subject is narrower than a class.

    Returns:
        ``"left:Firm"`` for a class, ``"left:Firm.firm_id"`` for an attribute.
    """
    return f"{scope}:{name}" if attr is None else f"{scope}:{name}.{attr}"


@dataclass(frozen=True)
class Cluster(Generic[DeclarationT]):
    """One group, over vertices or relations, in the sides' own names.

    ``left`` / ``right`` are the closed member sets: every class the group's
    equivalences name, and every class a vocabulary merges with one of them.
    ``declared_left`` / ``declared_right`` are the ones the equivalences
    name. ``declaration`` is one equivalence over the closed sets.
    """

    left: tuple[str, ...]
    right: tuple[str, ...]
    into: str
    declaration: DeclarationT
    aliases: dict[Side, dict[str, str]] = field(default_factory=dict)
    declared_into: str | None = None
    synthesized: bool = False
    declared_left: tuple[str, ...] = ()
    declared_right: tuple[str, ...] = ()

    def declared_members(self, side: Side) -> tuple[str, ...]:
        """The members the equivalences name on *side*, without the vocabulary-joined ones."""
        declared = self.declared_left if side == "left" else self.declared_right
        return declared or self.members(side)

    def members(self, side: Side) -> tuple[str, ...]:
        return self.left if side == "left" else self.right

    def resolved(self, side: Side, declared: str) -> str:
        """The member a spelling names on *side*: its own name, or its canonical one."""
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
    """Every group of one merge op, as merge applies them."""

    vertices: tuple[Cluster[VertexEquivalence], ...]
    relations: tuple[Cluster[RelationEquivalence], ...]

    @property
    def labels(self) -> frozenset[str]:
        """The merged names of every vertex cluster."""
        return frozenset(c.into for c in self.vertices)

    @property
    def relation_labels(self) -> frozenset[str]:
        """The merged names of every relation cluster."""
        return frozenset(c.into for c in self.relations)

    @property
    def declared_intos(self) -> frozenset[str]:
        """Every ``into`` the equivalences of the groups spell."""
        return frozenset(
            c.declared_into
            for c in (*self.vertices, *self.relations)
            if c.declared_into is not None
        )

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
