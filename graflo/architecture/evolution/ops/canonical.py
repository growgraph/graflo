"""Canonical vocabulary maps and the op that applies one."""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel

from .validation import (
    validate_rename_map_is_injective,
    validate_vocabulary_is_idempotent,
    validate_vocabulary_map,
    vocabulary_groups,
)


class CanonicalMap(ConfigBaseModel):
    """Declared translation of a source vocabulary into canonical names.

    A partial function on names, identity where unmapped: ``vertices`` maps
    source class names to canonical class names, ``relations`` does the same
    for relation names, and ``properties`` maps, per *source* class name,
    source attribute names to canonical attribute names — including for
    classes whose name does not change. Two sources sharing a target is a
    merge and must be acknowledged with ``allow_merges``.

    It is a **vocabulary**, so it is idempotent: a canonical name is a fixed
    point that no entry maps away from. A chain (``{X: Z, Z: Q}``) or a swap
    is refused at construction — that shape is a relabel, which
    :class:`CanonicalizeOp` expresses directly. The rule is what lets two
    maps, or a map and an equivalence, be checked for agreement without
    asking in which order they were written.

    Used on its own through :func:`~graflo.architecture.evolution.canonical.canonical_map_to_ops`,
    and on :attr:`MergeManifestsOp.canonical_maps` where it names the
    merged classes and is checked against the declared equivalences.
    """

    vertices: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Class rename map: ``{source_class: canonical_class}``.",
    )
    properties: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-source-class attribute rename map: "
            "``{source_class: {source_attr: canonical_attr}}``."
        ),
    )
    relations: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Relation rename map: ``{source_relation: canonical_relation}``.",
    )
    allow_merges: bool = PydanticField(
        default=False,
        description=(
            "Accept a non-injective ``vertices`` / ``relations`` map. Two "
            "sources sharing a canonical target is a *merge*, not a rename; "
            "it must be a stated intent because merging fuses entities and "
            "can create self-relations."
        ),
    )
    allow_dangling_entries: bool = PydanticField(
        default=False,
        description=(
            "Accept entries that name nothing in the manifest the map is "
            "applied to, dropping and logging each one. A shared vocabulary "
            "map is legitimately broader than any single manifest. Off by "
            "default, because a misspelt class has exactly the same shape, "
            "and dropping it silently narrows the rename to less than the "
            "author asked for."
        ),
    )
    allow_self_relations: bool = PydanticField(
        default=False,
        description=(
            "Accept a merge this map makes whose sources are connected by an "
            "edge that becomes a self-relation."
        ),
    )
    allow_observation_fusion: bool = PydanticField(
        default=False,
        description=(
            "Accept a merge this map makes whose sources are produced in one "
            "accumulator slot, fusing those observations into one node."
        ),
    )

    @model_validator(mode="after")
    def _validate_maps(self) -> CanonicalMap:
        if not self.allow_merges:
            # Identity entries (source == target) are excluded: a lowered
            # cluster map deliberately carries one for every member, including
            # the merged name itself, to declare it a member of its own group
            # -- that self entry must not read as a collision here. The op the
            # map lowers to counts it, which is where the merge is acknowledged.
            validate_rename_map_is_injective(
                {s: t for s, t in self.vertices.items() if s != t},
                kind="canonical vertex",
                merge_hint="CanonicalMap(allow_merges=True)",
            )
            validate_rename_map_is_injective(
                {s: t for s, t in self.relations.items() if s != t},
                kind="canonical relation",
                merge_hint="CanonicalMap(allow_merges=True)",
            )
        validate_vocabulary_is_idempotent(self.vertices, kind="canonical vertex")
        validate_vocabulary_is_idempotent(self.relations, kind="canonical relation")
        for source_class, attr_map in self.properties.items():
            validate_rename_map_is_injective(
                attr_map,
                kind=f"canonical property (class {source_class!r})",
                merge_hint="a transform that combines the fields upstream",
            )
            validate_vocabulary_is_idempotent(
                attr_map, kind=f"canonical property (class {source_class!r})"
            )
        return self

    def canonical_class(self, source_class: str) -> str:
        """Canonical name of *source_class* (itself when unmapped)."""
        return self.vertices.get(source_class, source_class)

    def canonical_relation(self, source_relation: str) -> str:
        """Canonical name of *source_relation* (itself when unmapped)."""
        return self.relations.get(source_relation, source_relation)

    @property
    def vertex_targets(self) -> set[str]:
        """Canonical class names this map establishes (targets of a real rename)."""
        return {t for s, t in self.vertices.items() if s != t}

    @property
    def relation_targets(self) -> set[str]:
        """Canonical relation names this map establishes."""
        return {t for s, t in self.relations.items() if s != t}

    def canonical_property_names(self, canonical_class: str) -> set[str]:
        """Canonical attribute names the map establishes on *canonical_class*."""
        names: set[str] = set()
        for source_class, attr_map in self.properties.items():
            if self.canonical_class(source_class) == canonical_class:
                names.update(new for old, new in attr_map.items() if old != new)
        return names


class CanonicalizeOp(ConfigBaseModel):
    """Relabel classes, attributes and relations by one vocabulary map, in one step.

    The map is a partial function on names — identity where unmapped — applied
    simultaneously over the original schema, so a chain (``{X: Z, Z: Q}``) and
    a swap resolve without an intermediate state, and the fibers of the map
    are exactly the groups that merge. A target that already exists and does
    not move must be declared a member of its own group with a self entry
    (``Company: Company``); otherwise the op refuses rather than merging into
    it silently. ``properties`` is keyed by the *source* class name and is
    applied before the class relabel.

    This is the single lowering of a
    :class:`~graflo.architecture.evolution.canonical.CanonicalMap`, and the
    per-side step of
    :func:`~graflo.architecture.evolution.merge.merge_manifests`.
    """

    op: Literal["canonicalize"] = "canonicalize"
    vertices: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Class map: ``{source_class: canonical_class}``.",
    )
    properties: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-source-class attribute map: "
            "``{source_class: {source_attr: canonical_attr}}``."
        ),
    )
    relations: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Relation map: ``{source_relation: canonical_relation}``.",
    )
    allow_merges: bool = PydanticField(
        default=False,
        description=(
            "Accept a group of more than one class or relation collapsing onto "
            "one target. A merge fuses entities and can create self-relations, "
            "so it is acknowledged here rather than inferred from the map."
        ),
    )
    allow_self_relations: bool = PydanticField(
        default=False,
        description=(
            "Accept a merge whose sources are connected by an edge that becomes "
            "a self-relation once both endpoints land on the same class."
        ),
    )
    allow_observation_fusion: bool = PydanticField(
        default=False,
        description=(
            "Accept a merge whose sources are produced in one accumulator slot "
            "(the same pipeline level and the same ``role``, or both bare), "
            "fusing those observations into one node."
        ),
    )

    @model_validator(mode="after")
    def _validate_map(self) -> CanonicalizeOp:
        validate_vocabulary_map(
            self.vertices,
            self.relations,
            self.properties,
            allow_merges=self.allow_merges,
            kind="canonicalize",
            merge_hint="set allow_merges=true",
        )
        return self

    @property
    def vertex_groups(self) -> dict[str, list[str]]:
        """``{target: [members]}`` over ``vertices``, self entries included."""
        return vocabulary_groups(self.vertices)

    @property
    def relation_groups(self) -> dict[str, list[str]]:
        """``{target: [members]}`` over ``relations``, self entries included."""
        return vocabulary_groups(self.relations)

    @property
    def merges(self) -> bool:
        """Whether any class or relation group has more than one member."""
        return any(
            len(members) > 1
            for groups in (self.vertex_groups, self.relation_groups)
            for members in groups.values()
        )
