"""The binary manifest merge op and its per-side renames."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel

from .canonical import CanonicalMap
from .equivalence import RelationEquivalence, VertexEquivalence
from .field_types import MergeFieldTypes
from .validation import validate_rename_map_is_injective


class SideRenames(ConfigBaseModel):
    """Renames applied to one side's names that no equivalence groups.

    Applied simultaneously with the side's groups and vocabulary, so a chain
    (``{Asset: WorkOrder, WorkOrder: Ticket}``) and a swap resolve without an
    intermediate name, and any name either side uses or vacates may be a
    target. Every source must exist on the side. A class or relation that an
    equivalence groups is named by that equivalence's ``into``, not here.
    """

    vertices: dict[str, str] = PydanticField(
        default_factory=dict, description="Class renames: ``{old: new}``."
    )
    relations: dict[str, str] = PydanticField(
        default_factory=dict, description="Relation renames: ``{old: new}``."
    )
    properties: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description=(
            "Attribute renames keyed by the class's own name: ``{class: {old: new}}``."
        ),
    )
    resources: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Resource renames, applied before the union: ``{old: new}``.",
    )

    @model_validator(mode="after")
    def _validate_renames(self) -> SideRenames:
        validate_rename_map_is_injective(
            {s: t for s, t in self.vertices.items() if s != t},
            kind="renames.vertices",
            merge_hint="a VertexEquivalence",
        )
        validate_rename_map_is_injective(
            {s: t for s, t in self.relations.items() if s != t},
            kind="renames.relations",
            merge_hint="a RelationEquivalence",
        )
        validate_rename_map_is_injective(
            {s: t for s, t in self.resources.items() if s != t},
            kind="renames.resources",
            merge_hint="distinct resource names",
        )
        for cls, attr_map in self.properties.items():
            validate_rename_map_is_injective(
                attr_map,
                kind=f"renames.properties (class {cls!r})",
                merge_hint="a transform that combines the fields upstream",
            )
        return self

    def is_empty(self) -> bool:
        """Whether no rename is declared."""
        return not (
            self.vertices or self.relations or self.properties or self.resources
        )


class MergeRenames(ConfigBaseModel):
    """Per-side renames of a merge, in each side's own names."""

    left: SideRenames = PydanticField(default_factory=SideRenames)
    right: SideRenames = PydanticField(default_factory=SideRenames)

    def __getitem__(self, side: str) -> SideRenames:
        return self.left if side == "left" else self.right


#: Keys a merge op no longer accepts, and what replaces each. Refused with the
#: replacement named rather than as an unknown field.
_REMOVED_MERGE_KEYS: dict[str, str] = {
    "vertices": "spell it `vertex_equivalences`",
    "relations": "spell it `relation_equivalences`",
    "resource_renames": "move it to `renames.right.resources`",
    "allow_merges": (
        "drop it: listing several members in an equivalence is the "
        "declaration, and a vocabulary acknowledges its own merges"
    ),
    "allow_self_relations": (
        "set `allow: [self_relations]` on the equivalence whose merge it accepts"
    ),
    "allow_observation_fusion": (
        "set `allow: [observation_fusion]` on the equivalence whose merge it accepts"
    ),
    "allow_row_fusion": (
        "set `allow: [observation_fusion]` on the equivalence whose merge it accepts"
    ),
}


class MergeManifestsOp(ConfigBaseModel):
    """Merge two full ``GraphManifest``s using explicit equivalence maps.

    Binary only — apply via :func:`~graflo.architecture.evolution.merge.merge_manifests`.
    Unary :func:`~graflo.architecture.evolution.apply.apply_evolution` rejects this op.

    Every name in the op is a name the input manifests declare. The
    vocabulary (``canonical_maps``), the equivalences and ``renames`` are
    resolved together, in one pass over those names, so nothing has to be
    written in an intermediate vocabulary. A group is everything an
    equivalence links, including every class a vocabulary merges with one of
    its members; its merged name is ``into``, else the vocabulary's name, else
    the one spelling its members share. Empty equivalences yield a disjoint
    union, subject to ``name_conflict``.

    A vertex equivalence's ``identity`` with a derived or ``local_key`` branch
    is applied to the merged union before return (canonical attributes →
    resource derivations → priority funnel), then the members' own keys are
    demoted to secondary identities named by each side's origin.
    """

    op: Literal["merge_manifests"] = "merge_manifests"
    vertex_equivalences: list[VertexEquivalence] = PydanticField(
        default_factory=list,
        description="Vertex equivalences across the two input manifests.",
    )
    relation_equivalences: list[RelationEquivalence] = PydanticField(
        default_factory=list,
        description="Relation equivalences across the two input manifests.",
    )
    renames: MergeRenames = PydanticField(
        default_factory=MergeRenames,
        description=(
            "Per-side renames of classes, relations, attributes and resources "
            "that no equivalence groups."
        ),
    )
    name: str | None = PydanticField(
        default=None,
        description=(
            "Label for the merged manifest and its schema. Unset, the two "
            "sides' names are folded into ``left+right``."
        ),
    )
    target_namespace: str | None = PydanticField(
        default=None,
        description=(
            "Database / graph / space the merged schema deploys into. "
            "Supersedes both sides' ``db_profile.target_namespace`` (so it "
            "also resolves a disagreement between them) and is validated "
            "against the merged ``db_flavor``. Unset, the namespace is "
            "derived from the schema name when deployed."
        ),
    )
    name_conflict: Literal["error", "prefix_right", "union_right"] = PydanticField(
        default="error",
        description=(
            "How to handle a name both sides arrive at that no equivalence "
            "covers (vertices, relations, resources, connectors). ``error`` "
            "refuses and names the equivalences to declare; ``prefix_right`` "
            "keeps them apart under ``r_`` names; ``union_right`` unions "
            "vertices and relations of exactly the same name, each pair "
            "becoming a synthesized 1-1 equivalence, so identity and property "
            "reconciliation apply as to a declared one (resources and "
            "connectors are addresses, not concepts, so it behaves as "
            "``error`` for them). Two spellings of one concept "
            "(``OrderLine`` / ``order_line``) are never unioned: ``error`` and "
            "``union_right`` refuse them, ``prefix_right`` keeps them apart."
        ),
    )
    router_scope: Literal["side", "union"] = PydanticField(
        default="side",
        description=(
            "What a ``vertex_router`` may route a discriminator value missing "
            "from its ``type_map`` to, after the merge. ``side`` closes each "
            "router over its own side's classes: merge writes every class of "
            "that side into the table, under its merged name, and sets "
            "``type_map_only``, so a value the side never modeled is skipped "
            "as it was before the merge. ``union`` leaves routers open: such a "
            "value can name any class of the merged schema, the other side's "
            "included -- for sources that share type names and ids."
        ),
    )
    allow_dangling_entries: bool = PydanticField(
        default=False,
        description=(
            "Accept canonical map entries that name nothing on the side they "
            "are scoped to, dropping and logging each one instead of refusing "
            "with the list. Set it on a map itself to say the map is broader "
            "than this merge; set it here to say so for both maps at once."
        ),
    )
    canonical_maps: dict[Literal["left", "right", "both"], CanonicalMap] = (
        PydanticField(
            default_factory=dict,
            description=(
                "The vocabulary per side. ``left`` / ``right`` apply to that "
                "manifest's own names, ``both`` to either. A vocabulary is the "
                "default name of a class; an equivalence's ``into`` overrides "
                "it for the whole group."
            ),
        )
    )
    field_types: MergeFieldTypes | None = PydanticField(
        default=None,
        description=(
            "Merged property types, keyed by merged names: every member that "
            "carries the property is retyped before the fold, so members that "
            "disagree on a type merge into this one."
        ),
    )
    origins: dict[Literal["left", "right"], str] | None = PydanticField(
        default=None,
        description=(
            "Each side's origin name; unset, the side's schema name. A re-keyed "
            "member's demoted key is named by it -- property ``<origin>__<field>`` "
            "and secondary identity ``<origin>`` -- as is an omitted "
            "``local_key`` tag. Needed only when the union names something by "
            "it: then it must be an identifier without ``__``, the two must "
            "differ, and no member may author a secondary identity of that name."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _refuse_removed_keys(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        removed = sorted(key for key in data if key in _REMOVED_MERGE_KEYS)
        if data.get("name_conflict") == "fuse_right":
            raise ValueError(
                "merge_manifests: name_conflict 'fuse_right' was removed; spell "
                "it 'union_right'"
            )
        if removed:
            raise ValueError(
                "merge_manifests: "
                + "; ".join(
                    f"`{key}` was removed: {_REMOVED_MERGE_KEYS[key]}"
                    for key in removed
                )
            )
        return data
