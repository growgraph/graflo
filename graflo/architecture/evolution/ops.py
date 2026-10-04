"""Typed manifest evolution operations.

Two field names recur across the models and mean two different things:
``into`` is *where existing names collapse* — the target of a merge, a
rename, or an equivalence (``MergeVerticesOp``, ``MergeEdgesOp``,
``VertexEquivalence``, ``RelationEquivalence``, ``PropertyEquivalence``);
``name`` is *what a new thing is called* — an attribute an identity branch
derives (``DerivedBranch``, ``LocalKeyBranch``). A model never uses one for
the other.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, field_validator, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.bindings.core import Bindings
from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.ingestion.steps.models import TransformGuardConfig
from graflo.architecture.contract.ingestion.steps.ref import EdgeStepRef
from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.architecture.graph_types import EdgeDirection, Index
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, normalize_inverse_table
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import (
    Field,
    FieldType,
    SecondaryIdentity,
    Vertex,
)
from graflo.onto import DBType


def validate_rename_map_is_injective(
    renames: dict[str, str], *, kind: str, merge_hint: str
) -> None:
    """Reject a rename map that would collapse two names onto one.

    A rename is a relabelling: it must not change how many types exist. Two sources
    sharing a target is a *merge*, and the merge ops exist precisely because merging
    needs decisions a rename cannot express — which properties survive, how identity
    combines, what happens to edges that become self-loops. Left unchecked the
    collapse is silent: the name-keyed lookup maps in ``VertexConfig`` / ``EdgeConfig``
    keep the last definition and the earlier one is shadowed but still serialized.
    """
    collisions: dict[str, list[str]] = {}
    for source, target in renames.items():
        collisions.setdefault(target, []).append(source)
    collapsed = {
        target: sorted(sources)
        for target, sources in collisions.items()
        if len(sources) > 1
    }
    if collapsed:
        detail = "; ".join(
            f"{target!r} is the target of {sources}"
            for target, sources in sorted(collapsed.items())
        )
        raise ValueError(
            f"{kind} rename map is not injective: {detail}. "
            f"Renaming cannot merge types — use {merge_hint}."
        )


def validate_merge_sources(sources: Sequence[str], into: str, *, kind: str) -> None:
    """Reject a merge whose sources repeat or include the target.

    The docstrings promise both; enforcing them at parse time means a
    serialized change set fails where it is read rather than where it is
    replayed, after the ops before it have already been applied.
    """
    if len(set(sources)) != len(sources):
        raise ValueError(f"{kind}: sources list a name more than once: {list(sources)}")
    if into in sources:
        raise ValueError(f"{kind}: `into` {into!r} must not appear in `sources`")


def vocabulary_groups(mapping: Mapping[str, str]) -> dict[str, list[str]]:
    """``{target: [sources]}`` — the fibers of a vocabulary map, self entries included."""
    groups: dict[str, list[str]] = {}
    for source, target in mapping.items():
        groups.setdefault(target, []).append(source)
    return groups


def validate_vocabulary_map(
    vertices: Mapping[str, str],
    relations: Mapping[str, str],
    properties: Mapping[str, Mapping[str, str]],
    *,
    allow_merges: bool,
    kind: str,
    merge_hint: str,
) -> None:
    """Reject an unacknowledged collapse in a vocabulary map.

    A vocabulary map is a function on names, so its groups are its fibers:
    every source of one target, **including** a self entry ``t: t`` that
    declares an existing ``t`` a member of its own group. A group of more than
    one name is a merge — it fuses entities and can create self-relations — so
    it must be acknowledged rather than inferred from the map. Per-class
    attribute maps are plain renames and must be injective outright.
    """
    if not allow_merges:
        for noun, mapping in (("class", vertices), ("relation", relations)):
            collapsed = {
                target: sorted(sources)
                for target, sources in vocabulary_groups(mapping).items()
                if len(sources) > 1
            }
            if collapsed:
                detail = "; ".join(
                    f"{target!r} is the target of {sources}"
                    for target, sources in sorted(collapsed.items())
                )
                raise ValueError(
                    f"{kind}: {noun} map collapses names: {detail}. A merge is a "
                    f"stated intent — {merge_hint}."
                )
    for source_class, attr_map in properties.items():
        validate_rename_map_is_injective(
            dict(attr_map),
            kind=f"{kind} property (class {source_class!r})",
            merge_hint="a transform that combines the fields upstream",
        )


def validate_vocabulary_is_idempotent(mapping: Mapping[str, str], *, kind: str) -> None:
    """Reject a chain or a swap: a canonical vocabulary has fixed points.

    A name that is both a source that moves and a target of another entry
    (``{X: Z, Z: Q}``, ``{A: B, B: A}``) makes the map non-idempotent — applying
    it twice is not applying it once — so it cannot be read as a vocabulary,
    where a canonical name is by definition one nothing maps away from. Such a
    map is a *relabel*, which :class:`CanonicalizeOp` expresses (simultaneous
    application over the original schema).
    """
    moving = {source for source, target in mapping.items() if source != target}
    targets = {target for source, target in mapping.items() if source != target}
    chained = sorted(moving & targets)
    if chained:
        raise ValueError(
            f"{kind}: {chained} are both a source that moves and a canonical "
            "target. A canonical vocabulary has fixed points — a chain or a "
            "swap is a relabel, which CanonicalizeOp expresses."
        )


class RemoveVerticesOp(ConfigBaseModel):
    """Remove logical vertices and cascade: edges, ingestion resources, bindings."""

    op: Literal["remove_vertices"] = "remove_vertices"
    names: list[str] = PydanticField(
        ...,
        description="Vertex type names to remove from the schema.",
        min_length=1,
    )


class MergeVerticesOp(ConfigBaseModel):
    """Merge source vertices into a single logical name (schema, edges, ingestion)."""

    op: Literal["merge_vertices"] = "merge_vertices"
    sources: list[str] = PydanticField(
        ...,
        description=(
            "Vertex type names to merge away. Must not include ``into``. "
            "Each name must exist in the schema before the merge."
        ),
        min_length=1,
    )
    into: str = PydanticField(
        ...,
        description=(
            "Resulting vertex type name. If it already exists, source vertices are "
            "merged into it. If it does not exist, a new vertex is built from all sources."
        ),
    )
    allow_self_relations: bool = PydanticField(
        default=False,
        description=(
            "Accept edges whose endpoints both land on ``into``. A self-relation makes "
            "both endpoints share one accumulator slot, so assembly merges observations "
            "that were previously separate nodes. Rejected unless set."
        ),
    )
    allow_observation_fusion: bool = PydanticField(
        default=False,
        validation_alias=AliasChoices("allow_observation_fusion", "allow_row_fusion"),
        description=(
            "Accept resource pipelines whose sources land in one accumulator slot — "
            "the same level and the same ``role``, or both bare. Those steps then "
            "write to one slot, fusing into one node what a single source document "
            "emitted as two vertex observations. Same-level steps with distinct "
            "``role``s never share a slot and need no acknowledgement. Rejected "
            "unless set. ``allow_row_fusion`` is accepted as a legacy alias."
        ),
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> MergeVerticesOp:
        validate_merge_sources(self.sources, self.into, kind="merge_vertices")
        return self


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


class RenameVertexPropertiesOp(ConfigBaseModel):
    """Rename vertex properties (and identity references) and propagate to ingestion.

    ``renames`` maps each vertex name to a per-vertex ``{old_field: new_field}`` map.
    Schema-side: rewrites ``Field.name``, ``vertex.identity``, and any DB profile
    structures that reference field names (``vertex_indexes``, ``edge_specs.indexes``).
    Ingestion-side: rewrites ``VertexActor.from`` so the doc still uses the OLD field
    name (injecting ``{new_field: old_field}`` when missing), rewrites
    ``TransformActor.rename`` values that target a renamed vertex field, and updates
    ``Resource.extra_weights`` / ``edge.vertex_weights`` (:class:`~graflo.architecture.graph_types.Weight`
    ``fields``, ``map``, and ``filter`` keys that address vertex observation columns).
    """

    op: Literal["rename_vertex_properties"] = "rename_vertex_properties"
    renames: dict[str, dict[str, str]] = PydanticField(
        ...,
        description=(
            "Per-vertex field rename map: ``{vertex_name: {old_field: new_field}}``."
        ),
        min_length=1,
    )


class RemoveVertexPropertiesOp(ConfigBaseModel):
    """Remove vertex properties and propagate pruning to ingestion/db profile references."""

    op: Literal["remove_vertex_properties"] = "remove_vertex_properties"
    removals: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-vertex field removal map: ``{vertex_name: [field_name, ...]}``."
        ),
        min_length=1,
    )


class AddVertexPropertiesOp(ConfigBaseModel):
    """Add vertex properties to existing logical vertex types.

    An entry may be a bare name or a full :class:`~graflo.architecture.schema.vertex.Field`.
    Bare names were the original shape and still mean what they meant -- an
    untyped property -- but they cannot express a property that arrives with a
    type and a grounding, which is what any op stream that adds a *measured* or
    *temporal* property has to say in one replayable step.
    """

    op: Literal["add_vertex_properties"] = "add_vertex_properties"
    additions: dict[str, list[str | Field]] = PydanticField(
        ...,
        description=(
            "Per-vertex property additions: ``{vertex_name: [field_name | Field, ...]}``."
        ),
        min_length=1,
    )

    def field_names(self, vertex: str) -> list[str]:
        """The names added to *vertex*, whichever shape they were written in."""
        return [
            entry if isinstance(entry, str) else entry.name
            for entry in self.additions.get(vertex, [])
        ]


class NaturalIdentityTarget(ConfigBaseModel):
    """Target a natural key: the named properties identify the vertex directly."""

    mode: Literal["natural"] = "natural"
    identity: list[str] = PydanticField(
        ...,
        description="Property names forming the new primary identity.",
        min_length=1,
    )


class HashIdentityTarget(ConfigBaseModel):
    """Target a hash identity: a deterministic synthetic ``id`` digested from fields."""

    mode: Literal["hash"] = "hash"
    hash_from: list[str] = PydanticField(
        ...,
        description="Source property names whose values are digested into ``id``.",
        min_length=1,
    )


class FunnelIdentityTarget(ConfigBaseModel):
    """Target an identity funnel: ordered fallback branches digested into ``id``.

    The general form of :class:`HashIdentityTarget` — a flat hash key is a funnel
    with one branch. Both resolve to identity mode ``hash``.
    """

    mode: Literal["funnel"] = "funnel"
    funnel: IdentityFunnel = PydanticField(
        ...,
        description="Ordered fallback branches; the first complete one wins.",
    )


class AssignedIdentityTarget(ConfigBaseModel):
    """Target an assigned identity: an intentional UUID primary key."""

    mode: Literal["assigned"] = "assigned"


class BlankIdentityTarget(ConfigBaseModel):
    """Target a blank identity: an auto-generated placeholder ID."""

    mode: Literal["blank"] = "blank"


IdentityTarget = Annotated[
    NaturalIdentityTarget
    | HashIdentityTarget
    | FunnelIdentityTarget
    | AssignedIdentityTarget
    | BlankIdentityTarget,
    PydanticField(discriminator="mode"),
]


class IdentityReplacement(ConfigBaseModel):
    """New identity policy for one vertex, plus what becomes of the old one."""

    to: IdentityTarget = PydanticField(
        ...,
        description="The identity policy this vertex should have after the op.",
    )
    retire: Literal["demote", "keep", "drop"] = PydanticField(
        default="demote",
        description=(
            "What happens to the old identity field-set. ``demote`` turns it into a "
            "secondary identity (lookup index follows automatically), ``keep`` leaves "
            "the fields as plain properties, ``drop`` removes them. Demotion is "
            "downgraded to ``keep`` when the old identity was synthetic (hash / "
            "assigned / blank) or already equals the new one."
        ),
    )
    retire_as: str | None = PydanticField(
        default=None,
        description=(
            "Name for the demoted secondary identity. Defaults to "
            "``retired_identity``. Only meaningful with ``retire: demote``."
        ),
    )
    endpoints: Literal["follow_new", "pin_to_retired"] = PydanticField(
        default="follow_new",
        description=(
            "How edge steps that match this vertex on its primary identity behave "
            "afterwards. ``follow_new`` (default) leaves them on the primary, so they "
            "match the new identity. ``pin_to_retired`` rewrites them to select the "
            "demoted secondary identity, preserving the previous matching behaviour "
            "for sources that only carry the old key. Requires ``retire: demote``."
        ),
    )

    @model_validator(mode="after")
    def _validate_endpoint_policy(self) -> IdentityReplacement:
        if self.endpoints == "pin_to_retired" and self.retire != "demote":
            raise ValueError(
                "endpoints: pin_to_retired requires retire: demote — there is no "
                "retired secondary identity to pin to otherwise"
            )
        if self.retire_as is not None and self.retire != "demote":
            raise ValueError("retire_as is only meaningful with retire: demote")
        return self


class ReplaceIdentityOp(ConfigBaseModel):
    """Replace the identity policy of one or more vertices.

    Covers both a change of identity *fields* and a change of identity *mode*
    (``natural`` / ``hash`` / ``assigned`` / ``blank``), because the cascade is the
    same in either case: the field-set that upserts changes, and everything that
    referenced the old one must be repointed or retired.

    Not covered: ``blank`` vertices cannot retire by demotion (they cannot declare
    secondary identities at all).
    """

    op: Literal["replace_identity"] = "replace_identity"
    replacements: dict[str, IdentityReplacement] = PydanticField(
        ...,
        validation_alias=AliasChoices("replacements", "vertices"),
        description=(
            "Per-vertex identity replacement: ``{vertex_name: replacement}``. "
            "``vertices`` is accepted as a legacy alias."
        ),
        min_length=1,
    )


class AddSecondaryIdentitiesOp(ConfigBaseModel):
    """Declare alternate lookup keys on existing vertices.

    Secondary identities are lookup-only: upserts keep using the primary identity.
    Each declared field-set automatically gains a non-unique index at
    :meth:`Schema.finish_init`, so this op is how an edge-only source gains a way to
    reference endpoints by a business key without touching the primary identity.
    """

    op: Literal["add_secondary_identities"] = "add_secondary_identities"
    additions: dict[str, list[SecondaryIdentity]] = PydanticField(
        ...,
        description=(
            "Per-vertex secondary identities to declare: "
            "``{vertex_name: [{name, fields}, ...]}``. A bare field list is accepted "
            "for each entry and auto-named."
        ),
        min_length=1,
    )


class RemoveSecondaryIdentitiesOp(ConfigBaseModel):
    """Withdraw alternate lookup keys, dropping their derived indexes.

    Rejected when a surviving edge step still selects the removed field-set — that
    step would have no way to resolve its endpoint.
    """

    op: Literal["remove_secondary_identities"] = "remove_secondary_identities"
    removals: dict[str, list[str | list[str]]] = PydanticField(
        ...,
        description=(
            "Per-vertex secondary identities to withdraw, addressed by name or by "
            "field list: ``{vertex_name: [name | [field, ...], ...]}``."
        ),
        min_length=1,
    )


class EdgeIdentitiesEntry(ConfigBaseModel):
    """New uniqueness keys for one edge triple."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    identities: list[list[str]] = PydanticField(
        ...,
        description=(
            "Replacement uniqueness keys. Each key lists fields that, together with "
            "the resolved endpoints, must be unique; the ``source`` / ``target`` "
            "tokens stand for the endpoints themselves. An empty list clears them."
        ),
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation


class ReplaceEdgeIdentitiesOp(ConfigBaseModel):
    """Replace the uniqueness keys of logical edges.

    The edge-side counterpart of :class:`ReplaceIdentityOp`. There is no retire policy:
    edge identities have no lookup plane to demote into. Non-endpoint tokens are merged
    into edge ``properties`` by ``Edge.finish_init``, as with authored identities.
    """

    op: Literal["replace_edge_identities"] = "replace_edge_identities"
    edges: list[EdgeIdentitiesEntry] = PydanticField(
        ...,
        description="Per-edge replacement uniqueness keys.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> ReplaceEdgeIdentitiesOp:
        edge_ids = [entry.edge_id() for entry in self.edges]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError(
                "replace_edge_identities entries must be unique by "
                "(source, target, relation)"
            )
        return self


class RenameVerticesOp(ConfigBaseModel):
    """Rename logical vertex names across schema, ingestion, and bindings."""

    op: Literal["rename_vertices"] = "rename_vertices"
    renames: dict[str, str] = PydanticField(
        ...,
        validation_alias=AliasChoices("renames", "vertices"),
        description=(
            "Vertex rename map: ``{old_vertex: new_vertex}``. Must be injective. "
            "``vertices`` is accepted as a legacy alias."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameVerticesOp:
        validate_rename_map_is_injective(
            self.renames,
            kind="rename_vertices",
            merge_hint="MergeVerticesOp(sources=[...], into=...)",
        )
        return self


class RenameRelationsOp(ConfigBaseModel):
    """Rename logical edge relation names across schema and ingestion."""

    op: Literal["rename_relations"] = "rename_relations"
    renames: dict[str, str] = PydanticField(
        ...,
        validation_alias=AliasChoices("renames", "relations"),
        description=(
            "Relation rename map: ``{old_relation: new_relation}``. Must be "
            "injective. ``relations`` is accepted as a legacy alias."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameRelationsOp:
        validate_rename_map_is_injective(
            self.renames,
            kind="rename_relations",
            merge_hint="MergeEdgesOp(sources=[...], into=...)",
        )
        return self


class RenameResourcesOp(ConfigBaseModel):
    """Rename ingestion resource names and bindings references."""

    op: Literal["rename_resources"] = "rename_resources"
    renames: dict[str, str] = PydanticField(
        ...,
        validation_alias=AliasChoices("renames", "resources"),
        description=(
            "Ingestion resource rename map: ``{old_resource: new_resource}``. Must "
            "be injective. ``resources`` is accepted as a legacy alias."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameResourcesOp:
        # IngestionModel already rejects duplicate resource names, so a collapsing
        # map fails downstream anyway — but with a message about the model rather
        # than about the op the author actually wrote.
        validate_rename_map_is_injective(
            self.renames,
            kind="rename_resources",
            merge_hint="MergeManifestsOp renames.<side>.resources",
        )
        return self


class EdgeSelector(ConfigBaseModel):
    """Schema edge triple selector matching :data:`~graflo.architecture.graph_types.EdgeId`."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches edges with no relation set.",
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation


def _validate_unique_edge_selectors(
    selectors: Sequence[EdgeSelector], *, kind: str
) -> None:
    edge_ids = [selector.edge_id() for selector in selectors]
    if len(edge_ids) != len(set(edge_ids)):
        raise ValueError(
            f"{kind} edges entries must be unique by (source, target, relation)"
        )


class RemoveEdgesOp(ConfigBaseModel):
    """Remove logical edges from schema, profile, and ingestion selectors.

    Two addressing forms, combinable in one op. ``relations`` removes a relation
    on every endpoint pair it occurs on. ``edges`` removes exactly the named
    triples, which is the only way to remove one pair of several sharing a
    relation, or an edge with no relation set at all.
    """

    op: Literal["remove_edges"] = "remove_edges"
    relations: list[str] = PydanticField(
        default_factory=list,
        description="Relation names to remove from edge definitions and references.",
    )
    edges: list[EdgeSelector] = PydanticField(
        default_factory=list,
        description="Edge triples ``(source, target, relation)`` to remove.",
    )

    @model_validator(mode="after")
    def _validate_targets(self) -> RemoveEdgesOp:
        if not self.relations and not self.edges:
            raise ValueError("remove_edges requires at least one of relations or edges")
        _validate_unique_edge_selectors(self.edges, kind="remove_edges")
        return self


class MergeEdgesOp(ConfigBaseModel):
    """Merge source relation names into a canonical relation name."""

    op: Literal["merge_edges"] = "merge_edges"
    sources: list[str] = PydanticField(
        ...,
        description="Relation names to merge away. Must not include ``into``.",
        min_length=1,
    )
    into: str = PydanticField(
        ...,
        description="Canonical relation name that receives all source relations.",
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> MergeEdgesOp:
        validate_merge_sources(self.sources, self.into, kind="merge_edges")
        return self


class RenameEdgePropertiesOp(ConfigBaseModel):
    """Rename edge properties for each relation across schema/profile/ingestion."""

    op: Literal["rename_edge_properties"] = "rename_edge_properties"
    renames: dict[str, dict[str, str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field rename map: "
            "``{relation_name: {old_field: new_field}}``."
        ),
        min_length=1,
    )


class RemoveEdgePropertiesOp(ConfigBaseModel):
    """Remove edge properties for each relation across schema/profile/ingestion."""

    op: Literal["remove_edge_properties"] = "remove_edge_properties"
    removals: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field removals: ``{relation_name: [field_name, ...]}``."
        ),
        min_length=1,
    )


class AddEdgePropertiesOp(ConfigBaseModel):
    """Add edge properties for each relation in schema/profile defaults.

    An entry may be a bare name (an untyped property) or a full
    :class:`~graflo.architecture.schema.vertex.Field`, as on
    :class:`AddVertexPropertiesOp`, so a typed or grounded edge property is
    one replayable step rather than an add followed by a type change.
    """

    op: Literal["add_edge_properties"] = "add_edge_properties"
    additions: dict[str, list[str | Field]] = PydanticField(
        ...,
        description=(
            "Per-relation edge property additions: "
            "``{relation_name: [field_name | Field, ...]}``."
        ),
        min_length=1,
    )

    def field_names(self, relation: str) -> list[str]:
        """The names added to *relation*, whichever shape they were written in."""
        return [
            entry if isinstance(entry, str) else entry.name
            for entry in self.additions.get(relation, [])
        ]


class DeclareEdgeInversesOp(ConfigBaseModel):
    """Declare inverse pairs and symmetric relations in ``edge_config``.

    Purely logical: it records how relation names read one fact from its two
    endpoints and creates nothing. ``inverses`` pairs two distinct names; a pair
    is unordered, so ``{a: b}``, ``{b: a}`` and ``{a: b, b: a}`` declare the same
    thing. ``symmetric`` names relations that are their own inverse. Together
    they must give every relation at most one inverse (no ``a: b`` with
    ``b: c``), in the op and against what is already declared.

    Realize a pair with :class:`AddInverseEdgesOp` (explicit, portable inverse
    edges) or :class:`SetNativeInversesOp` (TigerGraph maintains the pair) --
    never both for one relation. A symmetric relation is realized by its edges
    being undirected (:class:`SetEdgeDirectedOp`), which the schema requires.
    """

    op: Literal["declare_edge_inverses"] = "declare_edge_inverses"
    inverses: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Pairs to declare: ``{relation: inverse_relation}``, either order.",
    )
    symmetric: list[str] = PydanticField(
        default_factory=list,
        description="Relations to declare as their own inverse.",
    )

    @model_validator(mode="after")
    def _validate_table(self) -> DeclareEdgeInversesOp:
        if not self.inverses and not self.symmetric:
            raise ValueError(
                "declare_edge_inverses: nothing to declare; give inverses or symmetric"
            )
        normalize_inverse_table(
            self.inverses.items(), self.symmetric, kind="declare_edge_inverses"
        )
        return self


class RetractEdgeInversesOp(ConfigBaseModel):
    """Withdraw declared inverses, addressed by relation name.

    A paired relation retracts its whole pair (either side names it); a
    symmetric relation retracts its own declaration. Refused while a native
    inverse still realizes a pair: that would leave the database maintaining a
    type the schema no longer names. Explicit inverse edges are ordinary edges
    and survive a retraction; so does ``directed: false``.
    """

    op: Literal["retract_edge_inverses"] = "retract_edge_inverses"
    relations: list[str] = PydanticField(
        ...,
        description="Relations whose declaration is withdrawn (either side of a pair).",
        min_length=1,
    )


class AddInverseEdgesOp(ConfigBaseModel):
    """Realize declared inverse pairs as explicit logical edges (portable to every backend).

    Edges are derived from the relation map: for each directed edge ``(S, T, r)``
    whose relation has a declared pair ``inv``, adds the logical edge
    ``(T, S, inv)`` unless it exists and its physical spec, and sets
    ``emit_inverse`` on the edge steps that write ``r``, so the same rows write
    both. No step is generated: the inverse is mirrored at assembly, after the
    relation is resolved, which covers every way a step can name its relation.
    A resource that already writes the inverse with a step of its own is left
    alone. The pair must be declared first (:class:`DeclareEdgeInversesOp`);
    this op never declares. A symmetric relation has no inverse edge to add --
    its edges are undirected. Refused for a relation whose inverse is native
    (:class:`SetNativeInversesOp`), since both would store the same fact.

    Withdraw with :class:`RemoveEdgesOp` on the inverse edges: removing an edge
    clears the ``emit_inverse`` flags that fed it.
    """

    op: Literal["add_inverse_edges"] = "add_inverse_edges"
    relations: list[str] | None = PydanticField(
        default=None,
        description=(
            "Paired relations whose edges get their inverse edge. Omitted: every "
            "relation in ``edge_config.inverses``, both sides."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique(self) -> AddInverseEdgesOp:
        if self.relations is not None and len(set(self.relations)) != len(
            self.relations
        ):
            raise ValueError("add_inverse_edges: relations must be unique")
        return self


class SetNativeInversesOp(ConfigBaseModel):
    """Have the database maintain declared inverses (TigerGraph ``WITH REVERSE_EDGE``).

    Physical, not logical: adds relations to, or removes them from,
    ``db_profile.native_inverses``. Keyed by relation, as TigerGraph is: the
    reverse type belongs to the edge type, which spans every ``(S, T)`` pair of
    the relation. The reverse type is named by the declared pair, so the pair
    must be declared (:class:`DeclareEdgeInversesOp`). Refused for symmetric
    relations, where explicit inverse edges exist, on undirected edges, for a
    relation stored under several physical names, and on non-TigerGraph
    profiles.
    """

    op: Literal["set_native_inverses"] = "set_native_inverses"
    relations: list[str] = PydanticField(
        ...,
        description="Paired relations whose declared inverse the database maintains.",
        min_length=1,
    )
    enabled: bool = PydanticField(
        default=True,
        description="``False`` withdraws the native inverse.",
    )

    @model_validator(mode="after")
    def _validate_unique(self) -> SetNativeInversesOp:
        if len(set(self.relations)) != len(self.relations):
            raise ValueError("set_native_inverses: relations must be unique")
        return self


class SetInverseEmissionOp(ConfigBaseModel):
    """Set or clear ``emit_inverse`` on edge steps, addressed by position.

    The ingestion half of a materialized inverse, as a primitive: which steps
    mirror the edges they write into the declared inverse. A step is addressed
    by resource and :class:`EdgeStepRef` (``at`` / ``step`` / ``link``), so the
    op says exactly which steps change and nothing is inferred at replay time.

    Enabling is refused where the flag could never write anything -- a step
    naming exactly one edge whose relation has no declared pair, is symmetric,
    or has no declared inverse edge. A step whose relation comes from the data
    is accepted; it mirrors per document what has a materialized inverse.
    """

    op: Literal["set_inverse_emission"] = "set_inverse_emission"
    steps: dict[str, list[EdgeStepRef]] = PydanticField(
        ...,
        description="Edge steps whose flag changes: ``{resource: [step ref, ...]}``.",
        min_length=1,
    )
    enabled: bool = PydanticField(
        default=True,
        description="``False`` clears the flag.",
    )

    @model_validator(mode="after")
    def _validate_steps(self) -> SetInverseEmissionOp:
        empty = sorted(name for name, refs in self.steps.items() if not refs)
        if empty:
            raise ValueError(f"set_inverse_emission: no steps given for {empty}")
        for name, refs in self.steps.items():
            keys = [ref.sort_key for ref in refs]
            if len(set(keys)) != len(keys):
                raise ValueError(
                    f"set_inverse_emission: steps of {name!r} must be unique"
                )
        return self


class AddResourceTransformsOp(ConfigBaseModel):
    """Append transform steps to named resources' pipelines.

    The first op whose primary effect is ingestion: ``graph_schema`` is
    untouched. Steps land at the root level of each pipeline unless ``at``
    names a deeper one; at whichever level they land, actor type-priority
    sorting (transform runs before vertex extraction at the same level) makes
    the position safe.

    The level is load-bearing rather than cosmetic. An actor reads its
    transform buffer at its own ``LocationIndex`` with no ancestor fallback,
    and a ``descend`` subtree runs *before* its own level's transforms, so a
    step appended at the root is invisible to a vertex produced under a
    ``descend`` — and a transform whose declared inputs are missing skips
    silently by default. Target the level that produces the vertex.

    Steps may reference a registry transform via ``call.use`` (resolved
    against the manifest's existing ``ingestion_model.transforms`` union the
    op's own ``transforms``) or carry a fully inline ``call``
    (``module`` + ``foo`` + ``params``), which cannot collide by name.
    """

    op: Literal["add_resource_transforms"] = "add_resource_transforms"
    additions: dict[str, list[dict[str, Any]]] = PydanticField(
        ...,
        description=(
            "Per-resource transform steps to append: "
            "``{resource_name: [step_dict, ...]}``."
        ),
        min_length=1,
    )
    at: dict[str, list[int]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-resource pipeline level to append into: "
            "``{resource_name: [step_index, ...]}``. Each index must address a "
            "``descend`` step, descending one level per element; an omitted or "
            "empty path means the root level."
        ),
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description=(
            "Named transforms to register in ``ingestion_model.transforms`` "
            "for steps that reference them via ``call.use``. A name already "
            "registered with a different body is an error at apply time."
        ),
    )

    @model_validator(mode="after")
    def _validate_steps(self) -> AddResourceTransformsOp:
        from graflo.architecture.contract.ingestion.steps.models import (
            TransformActorConfig,
        )
        from graflo.architecture.contract.ingestion.steps.normalize import (
            normalize_actor_step,
        )

        for resource_name, steps in self.additions.items():
            if not steps:
                raise ValueError(
                    f"add_resource_transforms: empty step list for resource "
                    f"{resource_name!r}"
                )
            for step in steps:
                normalized = normalize_actor_step(step)
                if (
                    not isinstance(normalized, dict)
                    or normalized.get("type") != "transform"
                ):
                    raise ValueError(
                        f"add_resource_transforms: step for resource "
                        f"{resource_name!r} is not a transform step: {step!r}"
                    )
                TransformActorConfig.model_validate(normalized)
        stray = sorted(set(self.at) - set(self.additions))
        if stray:
            raise ValueError(
                f"add_resource_transforms: `at` names resources {stray} with no "
                "steps to append"
            )
        for resource_name, path in self.at.items():
            if any(index < 0 for index in path):
                raise ValueError(
                    f"add_resource_transforms: `at` path for resource "
                    f"{resource_name!r} has a negative index: {path}"
                )
        for proto in self.transforms:
            if not proto.name:
                raise ValueError(
                    "add_resource_transforms: registry transforms must define "
                    "a non-empty name"
                )
        return self


class EnsureExtractedFields(ConfigBaseModel):
    """Fields that must survive extraction for one vertex type at one level."""

    vertex: str = PydanticField(
        ...,
        description="The vertex type whose extraction must keep ``fields``.",
    )
    fields: list[str] = PydanticField(
        ...,
        min_length=1,
        description="Property names that must reach the extracted vertex document.",
    )
    at: list[int] = PydanticField(
        default_factory=list,
        description=(
            "Pipeline level holding the producing step, as ``descend`` step "
            "indices. Empty means the root level."
        ),
    )


class EnsureExtractedFieldsOp(ConfigBaseModel):
    """Widen a producing step's projection so named fields are not dropped.

    Needed because a ``vertex_router`` delivers differently from a ``vertex``
    step. The router builds its child ``VertexActor`` at
    ``lindex.extend((role, 0))``, where the transform buffer is empty, so
    derived fields reach the child only through the merged observation — that
    is, through passthrough or ``from``. A plain ``vertex`` step instead reads
    the buffer directly, which bypasses ``keep_fields`` and
    ``extraction_scope`` entirely.

    So on a router, ``extraction_scope: mapped_only`` or a ``keep_fields`` list
    that does not name the fields drops them silently. This op restores them:
    ``keep_fields`` gains the names, and under ``mapped_only`` the per-type
    ``vertex_from_map`` entry gains identity mappings — seeded from the
    router-level ``from`` when the entry does not exist yet, since creating it
    otherwise replaces the author's projection rather than extending it.

    A plain ``vertex`` step, or a router that restricts nothing, is a no-op:
    the fields already survive.
    """

    op: Literal["ensure_extracted_fields"] = "ensure_extracted_fields"
    additions: dict[str, list[EnsureExtractedFields]] = PydanticField(
        ...,
        description=(
            "Per-resource extraction guarantees: "
            "``{resource_name: [{vertex, fields, at}, ...]}``."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_entries(self) -> EnsureExtractedFieldsOp:
        for resource_name, entries in self.additions.items():
            if not entries:
                raise ValueError(
                    f"ensure_extracted_fields: empty entry list for resource "
                    f"{resource_name!r}"
                )
            for entry in entries:
                if any(index < 0 for index in entry.at):
                    raise ValueError(
                        f"ensure_extracted_fields: `at` path for resource "
                        f"{resource_name!r} has a negative index: {entry.at}"
                    )
        return self


class AddVerticesOp(ConfigBaseModel):
    """Introduce new logical vertex types.

    The unary counterpart to what :class:`MergeManifestsOp` can only do binarily.
    A replayable change set that cannot introduce a type could only ever describe a
    shrinking graph, which is why this exists alongside ``remove_vertices``.
    """

    op: Literal["add_vertices"] = "add_vertices"
    vertices: list[Vertex] = PydanticField(
        ...,
        description="Full vertex definitions, in the shape the schema block accepts.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_names(self) -> AddVerticesOp:
        names = [vertex.name for vertex in self.vertices]
        if len(names) != len(set(names)):
            raise ValueError("add_vertices entries must be unique by name")
        return self


class AddEdgesOp(ConfigBaseModel):
    """Introduce new logical edge relations between existing vertex types."""

    op: Literal["add_edges"] = "add_edges"
    edges: list[Edge] = PydanticField(
        ...,
        description="Full edge definitions, in the shape the schema block accepts.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_ids(self) -> AddEdgesOp:
        edge_ids = [(edge.source, edge.target, edge.relation) for edge in self.edges]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError(
                "add_edges entries must be unique by (source, target, relation)"
            )
        return self


class EdgeRetargetEntry(ConfigBaseModel):
    """Repoint one edge triple at a different source and/or target vertex type."""

    source: str = PydanticField(..., description="Current source vertex type name.")
    target: str = PydanticField(..., description="Current target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    new_source: str | None = PydanticField(
        default=None,
        description="Replacement source vertex type; omit to keep the current one.",
    )
    new_target: str | None = PydanticField(
        default=None,
        description="Replacement target vertex type; omit to keep the current one.",
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation

    def retargeted_edge_id(self) -> tuple[str, str, str | None]:
        return (
            self.new_source or self.source,
            self.new_target or self.target,
            self.relation,
        )

    @model_validator(mode="after")
    def _require_a_change(self) -> EdgeRetargetEntry:
        if self.new_source is None and self.new_target is None:
            raise ValueError(
                "retarget_edges requires at least one of new_source or new_target"
            )
        if self.retargeted_edge_id() == self.edge_id():
            raise ValueError(
                f"retarget_edges: edge {self.edge_id()} would not change endpoints"
            )
        return self


class RetargetEdgesOp(ConfigBaseModel):
    """Change which vertex types an edge connects, preserving everything else.

    Remove-plus-add would lose the edge's properties, uniqueness keys, ``directed``
    flag, and its ``db_profile`` physical spec. Retargeting rewrites the ``EdgeId``
    everywhere it is keyed instead: edge config, physical specs, and pipeline edge steps.
    """

    op: Literal["retarget_edges"] = "retarget_edges"
    edges: list[EdgeRetargetEntry] = PydanticField(
        ...,
        description="Per-edge endpoint retargeting.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> RetargetEdgesOp:
        edge_ids = [entry.edge_id() for entry in self.edges]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError(
                "retarget_edges entries must be unique by (source, target, relation)"
            )
        retargeted = [entry.retargeted_edge_id() for entry in self.edges]
        if len(retargeted) != len(set(retargeted)):
            raise ValueError(
                "retarget_edges entries must not collide after retargeting"
            )
        return self


class FieldTypeSpec(ConfigBaseModel):
    """Target logical type for one property."""

    type: FieldType | None = PydanticField(
        ...,
        description="New logical field type; ``None`` clears the declared type.",
    )
    item_type: FieldType | None = PydanticField(
        default=None,
        description="Element type, required when ``type`` is ``LIST``.",
    )

    @model_validator(mode="after")
    def _validate_item_type(self) -> FieldTypeSpec:
        if self.type == FieldType.LIST and self.item_type is None:
            raise ValueError("a LIST field type requires item_type")
        if self.type != FieldType.LIST and self.item_type is not None:
            raise ValueError("item_type is only meaningful for a LIST field type")
        return self


class ChangeFieldTypesOp(ConfigBaseModel):
    """Set the logical type of existing vertex or edge properties.

    Makes the differ's ``CHANGE_VERTEX_FIELD_TYPE`` / ``CHANGE_EDGE_FIELD_TYPE``
    authorable. Targets are validated against the profile's ``db_flavor`` so an
    unsupported type fails here rather than at define time.
    """

    op: Literal["change_field_types"] = "change_field_types"
    vertices: dict[str, dict[str, FieldTypeSpec]] = PydanticField(
        default_factory=dict,
        description="``{vertex_name: {field_name: {type, item_type}}}``.",
    )
    edges: dict[str, dict[str, FieldTypeSpec]] = PydanticField(
        default_factory=dict,
        description="``{relation_name: {field_name: {type, item_type}}}``.",
    )

    @model_validator(mode="after")
    def _require_a_target(self) -> ChangeFieldTypesOp:
        if not self.vertices and not self.edges:
            raise ValueError(
                "change_field_types requires at least one of vertices or edges"
            )
        return self


class AddVertexIndexesOp(ConfigBaseModel):
    """Author secondary indexes on vertices in the database profile."""

    op: Literal["add_vertex_indexes"] = "add_vertex_indexes"
    indexes: dict[str, Annotated[list[Index], PydanticField(min_length=1)]] = (
        PydanticField(
            ...,
            description="``{vertex_name: [Index, ...]}``.",
            min_length=1,
        )
    )


class RemoveVertexIndexesOp(ConfigBaseModel):
    """Withdraw authored vertex indexes, addressed by field list.

    Indexes derived from ``secondary_identities`` are not removable here — they would
    be re-registered by the next ``finish_init``. Use
    :class:`RemoveSecondaryIdentitiesOp` for those.
    """

    op: Literal["remove_vertex_indexes"] = "remove_vertex_indexes"
    indexes: dict[
        str,
        Annotated[
            list[Annotated[list[str], PydanticField(min_length=1)]],
            PydanticField(min_length=1),
        ],
    ] = PydanticField(
        ...,
        description="``{vertex_name: [[field, ...], ...]}``.",
        min_length=1,
    )


class EdgeIndexEntry(ConfigBaseModel):
    """Indexes for one edge physical spec."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    purpose: str | None = PydanticField(
        default=None,
        description="Physical variant purpose; ``None`` addresses the base spec.",
    )
    indexes: list[Index] = PydanticField(
        default_factory=list,
        description="Indexes to add to this spec.",
    )
    fields: list[list[str]] = PydanticField(
        default_factory=list,
        description="Field lists identifying indexes to remove from this spec.",
    )

    def physical_key(self) -> tuple[str, str, str | None, str | None]:
        return self.source, self.target, self.relation, self.purpose


def _validate_edge_index_entries(
    entries: Sequence[EdgeIndexEntry],
    *,
    kind: str,
    carries: Literal["indexes", "fields"],
) -> None:
    """Each entry addresses one spec, and carries the field the op reads.

    ``EdgeIndexEntry`` serves both ops, so the wrong field is silently ignored
    unless checked here.
    """
    keys = [entry.physical_key() for entry in entries]
    if len(keys) != len(set(keys)):
        raise ValueError(
            f"{kind} entries must be unique by (source, target, relation, purpose)"
        )
    empty = sorted(
        str(entry.physical_key()) for entry in entries if not getattr(entry, carries)
    )
    if empty:
        raise ValueError(f"{kind}: entries list no `{carries}`: {empty}")


class AddEdgeIndexesOp(ConfigBaseModel):
    """Author secondary indexes on edge physical specs."""

    op: Literal["add_edge_indexes"] = "add_edge_indexes"
    edges: list[EdgeIndexEntry] = PydanticField(
        ...,
        description="Per-spec indexes to add (``indexes`` on each entry).",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_entries(self) -> AddEdgeIndexesOp:
        _validate_edge_index_entries(
            self.edges, kind="add_edge_indexes", carries="indexes"
        )
        return self


class RemoveEdgeIndexesOp(ConfigBaseModel):
    """Withdraw authored indexes from edge physical specs, addressed by field list."""

    op: Literal["remove_edge_indexes"] = "remove_edge_indexes"
    edges: list[EdgeIndexEntry] = PydanticField(
        ...,
        description="Per-spec indexes to remove (``fields`` on each entry).",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_entries(self) -> RemoveEdgeIndexesOp:
        _validate_edge_index_entries(
            self.edges, kind="remove_edge_indexes", carries="fields"
        )
        return self


class SetEdgeDirectedOp(ConfigBaseModel):
    """Set the ``directed`` flag on logical edges.

    Small, but load-bearing for replay: ``directed`` decides what
    :class:`AddInverseEdgesOp` is allowed to duplicate, so an un-authorable flag makes
    inverse-edge change sets non-replayable.
    """

    op: Literal["set_edge_directed"] = "set_edge_directed"
    edges: list[EdgeSelector] = PydanticField(
        ...,
        description="Edge triples whose ``directed`` flag changes.",
        min_length=1,
    )
    directed: bool = PydanticField(
        ...,
        description="Value applied to every selected edge.",
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> SetEdgeDirectedOp:
        _validate_unique_edge_selectors(self.edges, kind="set_edge_directed")
        return self


class SetBindingsOp(ConfigBaseModel):
    """Replace the whole ``bindings`` block.

    The bindings block had **no op at all**, so ``diff_manifests`` could only
    report it as inexpressible and a change set that touched it was not
    replayable -- which is why a merge that unions two bindings registries
    could not be recorded as a commit.

    Wholesale rather than granular (add/remove/rename a connector) because that
    is what the diff needs to say: the block became this. The cost is
    coarseness in a three-way merge -- the whole block is one slot, so two
    independent bindings edits conflict where granular ops would merge. Granular
    connector ops can be added later without changing this one's meaning.
    """

    op: Literal["set_bindings"] = "set_bindings"
    bindings: Bindings | None = PydanticField(
        default=None,
        description=(
            "The bindings block after the op. ``None`` removes it, which is how "
            "the op stays total: every before/after pair is expressible."
        ),
    )


class SetDbProfileOp(ConfigBaseModel):
    """Replace the whole ``db_profile`` of the schema block.

    ``vertex_indexes`` and each edge spec's ``indexes`` already have four
    authoring ops; nothing else on the profile had any, so ``db_flavor``,
    ``target_namespace``, ``vertex_storage_names``, ``default_property_values``
    and the non-index parts of ``edge_specs`` were inexpressible -- and they are
    part of the content hash, so a change set that moved one of them could not
    replay.

    This op carries the **whole** profile, indexes included, and therefore
    subsumes the index ops when it is emitted; the differ emits it *instead of*
    them rather than alongside, so the two can never fight over ordering. When
    only indexes differ, the index ops are still what gets emitted -- they say
    more about intent and merge at a finer slot.
    """

    op: Literal["set_db_profile"] = "set_db_profile"
    profile: DatabaseProfile = PydanticField(
        ...,
        description="The database profile after the op, replacing the current one.",
    )


class SetVertexSemanticsOp(ConfigBaseModel):
    """Ground vertex types in an external vocabulary.

    Semantics were authorable only when a type was first written: no operation
    could attach an ``iri`` to a type that already existed, so a manifest that
    arrived ungrounded — an inferred one, or anything predating the block — could
    never be grounded through the op system, only rewritten by hand.

    Grounding is purely additive and never consulted at execution time, so this
    op cannot change how anything ingests or stores. What it changes is whether a
    reader who did not author the schema can tell what a type denotes.
    """

    op: Literal["set_vertex_semantics"] = "set_vertex_semantics"
    semantics: dict[str, Semantics | None] = PydanticField(
        ...,
        description=(
            "Per-vertex grounding: ``{vertex_name: Semantics}``. ``None`` clears "
            "the block, which is what makes the op invertible."
        ),
        min_length=1,
    )


class SetVertexDescriptionsOp(ConfigBaseModel):
    """Set or clear the human-readable description of existing vertex types.

    A description was authorable only when a type was first written, so a
    change to one -- a merge folding two types together and joining what each
    side said about it, or a correction to what an inferred type means -- had no
    operation and could only be diffed as inexpressible. Like grounding, a
    description is never consulted at execution time: this op cannot change how
    anything ingests or stores.
    """

    op: Literal["set_vertex_descriptions"] = "set_vertex_descriptions"
    descriptions: dict[str, str | None] = PydanticField(
        ...,
        description=(
            "Per-vertex description: ``{vertex_name: text}``. ``None`` clears it, "
            "which is what makes the op invertible."
        ),
        min_length=1,
    )


class SetEdgeSemanticsOp(ConfigBaseModel):
    """Ground edge relations in an external vocabulary.

    The vertex op's counterpart. Relations carry as much meaning as types --
    ``wasDerivedFrom`` and ``dependsOn`` are not interchangeable -- and a
    conformance profile that asks whether types are grounded has to be able to
    ask it of edges too.
    """

    op: Literal["set_edge_semantics"] = "set_edge_semantics"
    edges: list[EdgeSelector] = PydanticField(
        ...,
        description="Edge triples whose grounding changes.",
        min_length=1,
    )
    semantics: Semantics | None = PydanticField(
        default=None,
        description=(
            "Grounding applied to every selected edge; ``None`` clears it. Not "
            "``FieldSemantics``: a unit on an edge is meaningless, and the model "
            "split is what makes ``unit:`` here a validation error."
        ),
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> SetEdgeSemanticsOp:
        _validate_unique_edge_selectors(self.edges, kind="set_edge_semantics")
        return self


class FieldSemanticsTarget(ConfigBaseModel):
    """One property of one vertex, and the grounding to put on it."""

    vertex: str = PydanticField(..., description="Vertex type name.")
    field: str = PydanticField(..., description="Property name on that vertex.")
    semantics: FieldSemantics | None = PydanticField(
        default=None,
        description="Grounding for the property; ``None`` clears it.",
    )

    def key(self) -> tuple[Any, ...]:
        return ("vertex", self.vertex, self.field)


class EdgeFieldSemanticsTarget(ConfigBaseModel):
    """One property of one edge triple, and the grounding to put on it.

    Edge properties carry the same ``FieldSemantics`` as vertex properties --
    a ``since`` on an edge has a unit as much as a ``temperature`` on a vertex
    does -- but until this target existed no op could reach them.
    """

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    field: str = PydanticField(..., description="Property name on that edge.")
    semantics: FieldSemantics | None = PydanticField(
        default=None,
        description="Grounding for the property; ``None`` clears it.",
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation

    def key(self) -> tuple[Any, ...]:
        return ("edge", self.source, self.target, self.relation, self.field)


class SetFieldSemanticsOp(ConfigBaseModel):
    """Ground vertex and edge properties, including their unit of measure.

    Takes :class:`~graflo.architecture.schema.semantics.FieldSemantics` rather
    than :class:`~graflo.architecture.schema.semantics.Semantics`, which is the
    entire reason this is a third op rather than a mode of the vertex one: only
    a property may carry ``unit``, and the two models are kept apart so that
    ``unit:`` on a type is a validation error rather than a silent no-op.

    A target names either a vertex property (``vertex`` + ``field``) or an
    edge property (``source`` / ``target`` / ``relation`` + ``field``).
    """

    op: Literal["set_field_semantics"] = "set_field_semantics"
    targets: list[FieldSemanticsTarget | EdgeFieldSemanticsTarget] = PydanticField(
        ...,
        description="Properties whose grounding changes.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_targets(self) -> SetFieldSemanticsOp:
        keys = [t.key() for t in self.targets]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "set_field_semantics targets must be unique by (vertex, field) or "
                "(source, target, relation, field)"
            )
        return self


class AddResourcesOp(ConfigBaseModel):
    """Introduce ingestion resources, in the shape the ingestion block accepts.

    The unary way to grow ``ingestion_model``: without it a change set could
    only ever rename or narrow the resources it started with, and the differ
    had to report an added resource as inexpressible.
    """

    op: Literal["add_resources"] = "add_resources"
    resources: list[ResourceConfig] = PydanticField(
        ...,
        description="Full resource definitions.",
        min_length=1,
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description=(
            "Named transforms to register in ``ingestion_model.transforms`` for "
            "steps of the new resources that reference them via ``call.use``. "
            "Unioned by name exactly as ``add_resource_transforms`` does: an "
            "identical body already registered dedupes, a different one is an "
            "error at apply time."
        ),
    )

    @model_validator(mode="after")
    def _validate_unique_names(self) -> AddResourcesOp:
        names = [resource.name for resource in self.resources]
        if len(names) != len(set(names)):
            raise ValueError("add_resources entries must be unique by name")
        return self


class RemoveResourcesOp(ConfigBaseModel):
    """Remove ingestion resources and the bindings that wired them."""

    op: Literal["remove_resources"] = "remove_resources"
    names: list[str] = PydanticField(
        ...,
        description="Resource names to remove.",
        min_length=1,
    )


class ReplaceResourcesOp(ConfigBaseModel):
    """Replace the definitions of existing resources, matched by name.

    Each resource keeps its position in ``ingestion_model.resources`` and its
    bindings; only its definition changes. This is how an edited pipeline is
    expressed, since a pipeline is an ordered program no finer op can patch.
    """

    op: Literal["replace_resources"] = "replace_resources"
    resources: list[ResourceConfig] = PydanticField(
        ...,
        description="Full new definitions; each name must already exist.",
        min_length=1,
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description=(
            "Named transforms the new definitions reference via ``call.use``, "
            "registered as ``add_resources`` registers them."
        ),
    )

    @model_validator(mode="after")
    def _validate_unique_names(self) -> ReplaceResourcesOp:
        names = [resource.name for resource in self.resources]
        if len(names) != len(set(names)):
            raise ValueError("replace_resources entries must be unique by name")
        return self


class ProjectManifestOp(ConfigBaseModel):
    """Project a manifest to a vertex/edge subgraph with consistent cascade.

    Keeps only the requested logical vertices and edges (and optionally resources).
    All schema, ``db_profile``, ingestion, and bindings references to removed
    entities are pruned. Inverse edges are **not** kept by default; list them in
    ``keep_edges``, or set ``keep_inverse_edges`` to keep the declared mirror of
    every edge that is kept.

    With ``connectivity=\"induced_prune\"`` (v1 default), when ``keep_vertices`` is
    set, vertex types from that list with no incident surviving edge are dropped.

    ``depth`` turns ``keep_vertices`` from a literal list into seeds for a
    neighbourhood walk. One rule covers every combination: let ``E`` be
    ``keep_edges`` when given and every declared edge otherwise; the survivors are
    the vertex types within ``depth`` hops of a seed along ``E`` under
    ``direction``, and then ``E`` restricted to surviving endpoints. So the result
    is the *induced* subgraph on the hop ball — an edge between two neighbours
    survives even though no walk needed it — and ``keep_edges`` bounds the walk
    rather than being overridden by it. Pruning is unchanged: a seed left with no
    surviving edge is still dropped, whatever the depth.

    ``Edge.by`` (the third vertex type on an ``EdgeType.INDIRECT`` edge) is not part
    of schema adjacency, so a walk never pulls it in — the same blind spot the flat
    selection already has.
    """

    op: Literal["project_manifest"] = "project_manifest"
    keep_vertices: list[str] | None = PydanticField(
        default=None,
        description="Vertex type names to retain (after induced connectivity pruning).",
    )
    keep_edges: list[EdgeSelector] | None = PydanticField(
        default=None,
        description="Edge triples ``(source, target, relation)`` to retain.",
    )
    connectivity: Literal["induced_prune"] = PydanticField(
        default="induced_prune",
        description="How to interpret ``keep_vertices`` relative to surviving edges.",
    )
    depth: int = PydanticField(
        default=0,
        ge=0,
        description=(
            "Hops to expand ``keep_vertices`` by before the induced slice. "
            "``0`` (default) keeps the literal list."
        ),
    )
    direction: EdgeDirection = PydanticField(
        default=EdgeDirection.ANY,
        description=(
            "Orientation followed when expanding by ``depth``. Edges declared "
            "``directed: false`` are followed both ways regardless. Ignored when "
            "``depth`` is 0."
        ),
    )
    keep_resources: list[str] | None = PydanticField(
        default=None,
        description="Optional ingestion resource names to retain after graph slice.",
    )
    keep_inverse_edges: bool = PydanticField(
        default=False,
        description=(
            "With ``keep_edges``: also keep the declared mirror ``(T, S, inv)`` of "
            "each kept ``(S, T, r)``, so a materialized pair survives as a pair. "
            "Without ``keep_edges`` every edge between surviving vertices is kept "
            "already."
        ),
    )
    strict: bool = PydanticField(
        default=True,
        description="When True, unknown vertex/edge selectors raise ``ValueError``.",
    )
    partial_resources: Literal["trim", "drop"] = PydanticField(
        default="trim",
        description=(
            "A resource whose pipeline the projection shortens: ``trim`` keeps "
            "what survives and logs a warning naming each one; ``drop`` removes "
            "it with the bindings that served it, keeping only resources the "
            "projection leaves whole."
        ),
    )

    @model_validator(mode="after")
    def _validate_projection_selectors(self) -> ProjectManifestOp:
        if not self.keep_vertices and not self.keep_edges:
            raise ValueError(
                "project_manifest requires at least one of keep_vertices or keep_edges"
            )
        if self.keep_vertices and len(self.keep_vertices) != len(
            set(self.keep_vertices)
        ):
            raise ValueError("keep_vertices entries must be unique")
        if self.keep_edges:
            edge_ids = [selector.edge_id() for selector in self.keep_edges]
            if len(edge_ids) != len(set(edge_ids)):
                raise ValueError(
                    "keep_edges entries must be unique by (source, target, relation)"
                )
        if self.depth > 0 and not self.keep_vertices:
            # Without seeds `keep_vertices=None` already means "every vertex type",
            # so there is nothing for a walk to expand — the request is a mistake
            # rather than a no-op, and saying so beats silently ignoring `depth`.
            raise ValueError("project_manifest: depth > 0 requires keep_vertices")
        return self


class SanitizeOp(ConfigBaseModel):
    """Record the physical names a target flavor needs in ``DatabaseProfile``.

    Vertex storage names, relation names and vertex/edge property names that the
    flavor cannot store (reserved word, invalid character, forbidden prefix) get
    a stored name in the profile, deduplicated within each database namespace.
    The logical schema and the ingestion model are left untouched: a backend
    naming constraint is a physical fact, so it never renames a logical property.
    Idempotent.
    """

    op: Literal["sanitize"] = "sanitize"
    db_flavor: DBType = PydanticField(
        ...,
        description="Target database flavor whose reserved words/constraints drive the sanitization.",
    )
    reserved_words: list[str] | None = PydanticField(
        default=None,
        description=(
            "Optional override for the flavor's reserved words. "
            "When unset, ``graflo.db.util.load_reserved_words(db_flavor)`` is used."
        ),
    )


class DerivationSpec(ConfigBaseModel):
    """How one resource derives a canonical attribute from its raw doc fields."""

    input: list[str] = PydanticField(
        ...,
        min_length=1,
        description=(
            "RAW source-doc field names fed to the function, in order. "
            "Documents keep their original keys after property renames, so "
            "canonical property names are usually wrong here."
        ),
    )
    module: str = PydanticField(
        default="graflo.util.transform",
        description="Module holding the derivation function.",
    )
    foo: str = PydanticField(
        default="normalized_key",
        description=(
            "Function name; called as ``foo(*values, **params)``. The default "
            "trims and casefolds one value."
        ),
    )
    params: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Keyword parameters for the function.",
    )
    when: TransformGuardConfig | None = PydanticField(
        default=None,
        description=(
            "Run the step only for documents whose RAW field holds one of the "
            "listed values (``{field, in}``). A merge derives this guard from "
            "how the resource produces the class; one set here replaces it. "
            "Not allowed on a spec keyed by member: the member decides."
        ),
    )


class LocalKeySource(ConfigBaseModel):
    """Where one resource's side-local key comes from, and its namespace tag.

    The tag is what keeps records of different sources apart once they fail to
    fuse: ``f2`` from one source and ``f2`` from another are different
    entities, and ``a:f2`` / ``b:f2`` say so. It is required so that opting
    out is a statement, not an omission: ``tag=None`` (stored as ``""``, the
    neutral element, so it survives serialization) keeps the raw value as the
    local key with no separator — the author's claim that the values are
    already unique across every source of the class (UUIDs, IRIs, ids the
    source itself prefixes).
    """

    field: str = PydanticField(
        ...,
        description="RAW doc field carrying the side-local key.",
    )
    tag: str = PydanticField(
        ...,
        description=(
            "Namespace tag: tag 'a' turns 'f2' into 'a:f2'. ``None`` or ``\"\"`` "
            "keeps the raw value, no separator — only for values already "
            "unique across every source of the class."
        ),
    )
    when: TransformGuardConfig | None = PydanticField(
        default=None,
        description=(
            "Same as :attr:`DerivationSpec.when`: an explicit guard replacing "
            "the one the merge derives. Not allowed on a source keyed by member."
        ),
    )

    @field_validator("tag", mode="before")
    @classmethod
    def _none_is_the_empty_tag(cls, value: Any) -> Any:
        return "" if value is None else value


def _refuse_member_keyed_guards(
    kind: str, sources: Mapping[str, Any], label: str
) -> None:
    """A spec keyed by member may not carry ``when``: the member decides."""
    for resource, entry in sources.items():
        if not isinstance(entry, dict):
            continue
        if not entry:
            raise ValueError(
                f"{kind} {label!r}: resource {resource!r} keys its sources by "
                "member but names none"
            )
        guarded = sorted(m for m, spec in entry.items() if spec.when is not None)
        if guarded:
            raise ValueError(
                f"{kind} {label!r}: member-keyed sources for resource "
                f"{resource!r} set `when` on {guarded}; the member already "
                "decides, and the guard is derived from how the resource "
                "produces it"
            )


class DerivedBranch(ConfigBaseModel):
    """A funnel branch over an attribute each source derives from its own columns.

    ``name`` is the canonical attribute the branch keys on. ``sources`` is
    keyed by resource, because derivation inputs are that resource's raw
    column names. An entry is either

    * one :class:`DerivationSpec` — the resource derives the attribute the
      same way for every record of the class it produces; or
    * a dict keyed by member class — the resource produces several members
      and which one a record *is* decides the derivation. A member is keyed by
      its own name on its side or by its canonical one.

    The merge reads how the resource produces the class on its side and guards
    each step with ``when``: a member-keyed spec on the discriminator values
    that route onto its member, a single spec behind a ``vertex_router`` on
    the values that route onto the class, and nothing for a plain ``vertex``
    step. An explicit :attr:`DerivationSpec.when` replaces the derived guard.
    """

    name: str = PydanticField(
        ...,
        description="Canonical attribute the branch keys on; its funnel branch id.",
    )
    sources: dict[str, DerivationSpec | dict[str, DerivationSpec]] = PydanticField(
        ...,
        min_length=1,
        description=(
            "Per-resource derivation: ``{resource: spec}``, or ``{resource: "
            "{member_class: spec}}`` when the member a document is must decide "
            "the derivation."
        ),
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> DerivedBranch:
        _refuse_member_keyed_guards("derived branch", self.sources, self.name)
        return self

    def specs_for(self, resource: str) -> list[DerivationSpec]:
        """Derivations *resource* contributes to this branch, in order."""
        spec = self.sources.get(resource)
        if spec is None:
            return []
        return [spec] if isinstance(spec, DerivationSpec) else list(spec.values())

    def members_for(self, resource: str) -> list[str] | None:
        """Member classes keying *resource*'s specs, or ``None`` if unkeyed."""
        spec = self.sources.get(resource)
        return list(spec) if isinstance(spec, dict) else None


class LocalKeyBranch(ConfigBaseModel):
    """The last funnel branch: each source's own key behind a namespace tag.

    A record that completes no earlier branch keys on this one, so it is still
    written — as its own vertex, not joined with records of another source.
    ``local_key`` maps each resource to the column carrying its own key, or,
    like :attr:`DerivedBranch.sources`, to one such source per member class.
    """

    local_key: dict[str, LocalKeySource | dict[str, LocalKeySource]] = PydanticField(
        ...,
        min_length=1,
        description=(
            "Per-resource local-key wiring: ``{resource: source}``, or "
            "``{resource: {member_class: source}}`` when the member decides."
        ),
    )
    name: str = PydanticField(
        default="local_key",
        description="Canonical fallback property name on the class.",
    )
    sep: str = PydanticField(
        default=":",
        description="Separator between tag and key.",
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> LocalKeyBranch:
        _refuse_member_keyed_guards("local_key branch", self.local_key, self.name)
        return self

    @property
    def sources(self) -> dict[str, LocalKeySource | dict[str, LocalKeySource]]:
        """The per-resource wiring, under the name :class:`DerivedBranch` uses."""
        return self.local_key

    def sources_for(self, resource: str) -> list[LocalKeySource]:
        """Local-key sources *resource* contributes, in order."""
        source = self.local_key.get(resource)
        if source is None:
            return []
        return [source] if isinstance(source, LocalKeySource) else list(source.values())

    def members_for(self, resource: str) -> list[str] | None:
        """Member classes keying *resource*'s sources, or ``None`` if unkeyed."""
        source = self.local_key.get(resource)
        return list(source) if isinstance(source, dict) else None


#: A funnel branch over canonical properties the members already carry: one
#: name, or a composite ``[a, b]`` whose fields must all be present.
RawBranch = str | list[str]

#: One entry of :attr:`VertexEquivalence.identity`.
IdentityBranchDecl = RawBranch | DerivedBranch | LocalKeyBranch


def branch_fields(branch: IdentityBranchDecl) -> list[str]:
    """The canonical fields *branch* keys on."""
    if isinstance(branch, str):
        return [branch]
    if isinstance(branch, list):
        return list(branch)
    return [branch.name]


def branch_id(branch: IdentityBranchDecl) -> str:
    """The funnel branch id *branch* lowers to — part of the digest payload."""
    return "_".join(branch_fields(branch))


def identity_branches_funnel(branches: Sequence[IdentityBranchDecl]) -> IdentityFunnel:
    """The funnel *branches* declare, in declared order."""
    return IdentityFunnel(
        branches=[
            IdentityBranch(id=branch_id(branch), fields=branch_fields(branch))
            for branch in branches
        ]
    )


def check_identity_branches(
    branches: Sequence[IdentityBranchDecl], *, label: str
) -> None:
    """Refuse a branch list no funnel can be built from, naming *label*.

    Non-empty; no empty composite; at most one ``local_key``, and it last --
    every record completes it, so a branch after it never fires; unique
    branch ids, since they take part in the digest; and no derived name that
    a property branch keys on, which the derivation would overwrite.
    """
    if not branches:
        raise ValueError(
            f"{label}: identity lists no branch; omit it to carry the members' "
            "shared key through"
        )
    for branch in branches:
        if isinstance(branch, list) and not branch:
            raise ValueError(f"{label}: an identity branch is empty")
    local = [i for i, b in enumerate(branches) if isinstance(b, LocalKeyBranch)]
    if len(local) > 1:
        raise ValueError(f"{label}: identity has more than one local_key")
    if local and local[0] != len(branches) - 1:
        raise ValueError(
            f"{label}: the local_key branch must be the last one; a record keys "
            "on its first complete branch, and every record completes the local key"
        )
    ids = [branch_id(branch) for branch in branches]
    repeated = sorted({i for i in ids if ids.count(i) > 1})
    if repeated:
        raise ValueError(f"{label}: identity repeats the branches {repeated}")
    raw_fields = {
        f for b in branches if isinstance(b, str | list) for f in branch_fields(b)
    }
    derived = [
        b.name for b in branches if isinstance(b, DerivedBranch | LocalKeyBranch)
    ]
    shadowing = sorted(set(derived) & raw_fields)
    if shadowing:
        raise ValueError(
            f"{label}: derived branches {shadowing} are named like properties "
            "another branch keys on; the derivation would overwrite them"
        )


def _describe_branch_shapes() -> str:
    return (
        "a branch is a property name, a list of names, `{name, sources}`, or "
        "`{local_key: {resource: {field, tag}}}`"
    )


def _member_list(value: str | list[str]) -> list[str]:
    """Normalize a ``str | list[str]`` equivalence side to a list of members."""
    return [value] if isinstance(value, str) else list(value)


class PropertyEquivalence(ConfigBaseModel):
    """Align a property from the left and/or right member(s) onto a canonical name.

    At least one of ``left`` / ``right`` must be set. A bare string applies to
    every member declared on that side of the owning
    :class:`VertexEquivalence`; a ``{member: field}`` dict maps per member,
    for when members are not aligned under the same source field name.

    Exact-name matches do **not** need a :class:`PropertyEquivalence`: after
    boundary rename, ``merge_vertex_models`` unions fields by spelling, so a
    property present under the same name on every member fuses for free.
    Declare an equivalence only to rename or to pick a different ``into``; the
    merged key is declared on the :class:`VertexEquivalence`.
    """

    left: str | dict[str, str] | None = PydanticField(
        default=None,
        description=(
            "Field name on the left member(s): a bare string applies to every "
            "left member of the owning equivalence, a ``{member: field}`` dict "
            "maps per member."
        ),
    )
    right: str | dict[str, str] | None = PydanticField(
        default=None,
        description="Same shape as ``left``, for the right member(s).",
    )
    into: str = PydanticField(
        ...,
        description="Canonical property name on the merged vertex.",
    )

    @model_validator(mode="after")
    def _require_side(self) -> PropertyEquivalence:
        if self.left is None and self.right is None:
            raise ValueError(
                "PropertyEquivalence requires at least one of left or right"
            )
        for side, spec in (("left", self.left), ("right", self.right)):
            if isinstance(spec, dict) and not spec:
                raise ValueError(
                    f"PropertyEquivalence: {side} is an empty per-member map"
                )
        return self


class VertexEquivalence(ConfigBaseModel):
    """Collapse one or more left classes and one or more right classes into one.

    GraFlo applies this map deterministically; it does not infer semantic
    matches. ``left`` / ``right`` accept a bare class name (a 1-1 equivalence)
    or a list (an n-ary cluster): ``{Company, Shop} ~ {Org, Branch} ->
    Company``. A member may also be spelled by the canonical name a vocabulary
    gives it, which stands for every class the vocabulary sends there.

    The group is everything the equivalence links: its members, and every
    class a vocabulary merges with one of them. Per-member maps (property
    equivalences, member-keyed identity sources) may name any class of the
    group by its own name.

    Properties with the same spelling on every member after alignment fuse by
    exact name without an entry in ``properties`` — list only renames.

    ``identity`` is the merged key, as ordered funnel branches in canonical
    names. A branch is a property the members carry (a name, or a composite
    ``[a, b]``), a :class:`DerivedBranch` each source computes from its own
    columns, or a :class:`LocalKeyBranch` (the tagged fallback, last). One
    property branch keys the class on that natural key; anything else keys it
    on a funnel, where a record keys on its first complete branch.
    """

    left: str | list[str] = PydanticField(
        ..., description="One or more left-manifest vertex type names."
    )
    right: str | list[str] = PydanticField(
        ..., description="One or more right-manifest vertex type names."
    )
    into: str | None = PydanticField(
        default=None,
        description=(
            "Merged vertex type name: any name, including one either side "
            "uses or vacates. Never translated by a vocabulary; when it differs "
            "from the vocabulary's name for the group it wins. Omitted, the "
            "name comes from the vocabulary, or from the one spelling every "
            "member shares."
        ),
    )
    properties: list[PropertyEquivalence] = PydanticField(
        default_factory=list,
        description="Property alignment map applied before the vertex merge.",
    )
    identity: list[IdentityBranchDecl] | None = PydanticField(
        default=None,
        description=(
            "The merged key, as ordered funnel branches in canonical names: a "
            "property the members carry, a composite ``[a, b]``, a derived "
            "branch ``{name, sources}``, or ``{local_key: ...}`` (last). One "
            "property branch is a natural key; anything else is a funnel. When "
            "unset, identity is carried through only if every member agrees; "
            "disagreement raises `MergeIdentityError`."
        ),
    )
    derive_at: dict[str, list[int]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-resource pipeline level the derived and local-key branches "
            "derive at, as ``descend`` step indices. Omitted resources resolve "
            "to the single level producing the class (for member-keyed "
            "sources: the level producing the member on its side); supply a "
            "path only when a resource produces it at more than one level."
        ),
    )
    retire: Literal["demote", "keep"] = PydanticField(
        default="demote",
        description=(
            "What becomes of each member's pre-merge key once `identity` "
            "re-keys the merged class. `demote` keeps each as a lookup-only "
            "secondary identity on `into`, and points edge steps of resources "
            "that only reference a member at it; `keep` leaves the fields as "
            "plain properties. Unused while the merged class keeps its "
            "members' shared key."
        ),
    )
    allow: list[Literal["self_relations", "observation_fusion"]] = PydanticField(
        default_factory=list,
        description=(
            "Consequences of this merge it accepts. `self_relations`: an edge "
            "between two members becomes an edge from the class to itself. "
            "`observation_fusion`: two members produced in one accumulator "
            "slot (same pipeline level and `role`, or both bare) fuse into one "
            "node."
        ),
    )

    @field_validator("identity", mode="before")
    @classmethod
    def _branch_shapes(cls, value: Any) -> Any:
        """Name the four branch shapes instead of pydantic's union error."""
        if not isinstance(value, list):
            return value
        for entry in value:
            if isinstance(entry, str | DerivedBranch | LocalKeyBranch):
                continue
            if isinstance(entry, list) and all(isinstance(f, str) for f in entry):
                continue
            if isinstance(entry, dict) and ("sources" in entry or "local_key" in entry):
                continue
            raise ValueError(
                f"VertexEquivalence: identity branch {entry!r} has no known "
                f"shape; {_describe_branch_shapes()}"
            )
        return value

    def raw_branches(self) -> list[tuple[str, ...]]:
        """The property branches of ``identity``, each as its field tuple."""
        return [
            tuple(branch_fields(branch))
            for branch in self.identity or []
            if isinstance(branch, str | list)
        ]

    def derived_branches(self) -> list[DerivedBranch]:
        """The derived branches of ``identity``, in priority order."""
        return [b for b in self.identity or [] if isinstance(b, DerivedBranch)]

    def local_key_branch(self) -> LocalKeyBranch | None:
        """The ``local_key`` branch of ``identity``, if declared."""
        return next(
            (b for b in self.identity or [] if isinstance(b, LocalKeyBranch)), None
        )

    @property
    def has_derivation(self) -> bool:
        """Whether ``identity`` needs pipeline steps: a derived or local-key branch."""
        return any(
            isinstance(b, DerivedBranch | LocalKeyBranch) for b in self.identity or []
        )

    @property
    def left_members(self) -> list[str]:
        return _member_list(self.left)

    @property
    def right_members(self) -> list[str]:
        return _member_list(self.right)

    def members(self, side: Literal["left", "right"]) -> list[str]:
        return self.left_members if side == "left" else self.right_members

    def property_maps(
        self, side: Literal["left", "right"]
    ) -> dict[str, dict[str, str]]:
        """``{member: {old_field: into_field}}`` for *side*, bare strings expanded."""
        member_names = self.members(side)
        out: dict[str, dict[str, str]] = {}
        for pe in self.properties:
            spec = pe.left if side == "left" else pe.right
            if spec is None:
                continue
            per_member = (
                dict.fromkeys(member_names, spec) if isinstance(spec, str) else spec
            )
            for member, old in per_member.items():
                if old == pe.into:
                    continue
                bucket = out.setdefault(member, {})
                existing = bucket.get(old)
                if existing is not None and existing != pe.into:
                    raise ValueError(
                        f"VertexEquivalence: {side}:{member}.{old!r} would rename "
                        f"to both {existing!r} and {pe.into!r}"
                    )
                bucket[old] = pe.into
        return out

    @model_validator(mode="after")
    def _validate_members(self) -> VertexEquivalence:
        for side, members in (
            ("left", self.left_members),
            ("right", self.right_members),
        ):
            if not members:
                raise ValueError(
                    f"VertexEquivalence: {side} must name at least one class"
                )
            if len(members) != len(set(members)):
                raise ValueError(
                    f"VertexEquivalence: {side} lists a class more than once: {members}"
                )
        if len(self.allow) != len(set(self.allow)):
            raise ValueError(
                f"VertexEquivalence: allow lists a value twice: {self.allow}"
            )
        # Per-member property maps may name any class of the group, which only
        # the manifests and the vocabulary decide: checked at resolution.
        return self

    @model_validator(mode="after")
    def _validate_identity(self) -> VertexEquivalence:
        if self.identity is None:
            if self.derive_at:
                raise ValueError(
                    "VertexEquivalence: derive_at is set but no identity branch "
                    "derives anything"
                )
            return self
        check_identity_branches(self.identity, label="VertexEquivalence")
        if self.derive_at and not self.has_derivation:
            raise ValueError(
                "VertexEquivalence: derive_at is set but no identity branch "
                "derives anything"
            )
        return self


class RelationEquivalence(ConfigBaseModel):
    """Collapse one or more left relations and one or more right relations onto one name.

    Shares the ``left`` / ``right`` n-ary shape and the naming rules of
    :class:`VertexEquivalence`.
    """

    left: str | list[str] = PydanticField(
        ..., description="One or more left relation names."
    )
    right: str | list[str] = PydanticField(
        ..., description="One or more right relation names."
    )
    into: str | None = PydanticField(
        default=None,
        description=(
            "Merged relation name, never translated by a vocabulary. Omitted, "
            "the name comes from the vocabulary, or from the one spelling "
            "every member shares."
        ),
    )

    @property
    def left_members(self) -> list[str]:
        return _member_list(self.left)

    @property
    def right_members(self) -> list[str]:
        return _member_list(self.right)

    def members(self, side: Literal["left", "right"]) -> list[str]:
        return self.left_members if side == "left" else self.right_members

    @model_validator(mode="after")
    def _validate_members(self) -> RelationEquivalence:
        for side, members in (
            ("left", self.left_members),
            ("right", self.right_members),
        ):
            if not members:
                raise ValueError(
                    f"RelationEquivalence: {side} must name at least one relation"
                )
            if len(members) != len(set(members)):
                raise ValueError(
                    f"RelationEquivalence: {side} lists a relation more than once: {members}"
                )
        return self


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
    demoted to secondary identities.
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


ManifestOp = Annotated[
    RemoveVerticesOp
    | AddResourceTransformsOp
    | EnsureExtractedFieldsOp
    | AddResourcesOp
    | RemoveResourcesOp
    | ReplaceResourcesOp
    | AddVerticesOp
    | AddEdgesOp
    | RetargetEdgesOp
    | AddSecondaryIdentitiesOp
    | RemoveSecondaryIdentitiesOp
    | ReplaceEdgeIdentitiesOp
    | ChangeFieldTypesOp
    | AddVertexIndexesOp
    | RemoveVertexIndexesOp
    | AddEdgeIndexesOp
    | RemoveEdgeIndexesOp
    | SetEdgeDirectedOp
    | SetBindingsOp
    | SetDbProfileOp
    | SetVertexSemanticsOp
    | SetVertexDescriptionsOp
    | SetEdgeSemanticsOp
    | SetFieldSemanticsOp
    | MergeVerticesOp
    | CanonicalizeOp
    | RenameVertexPropertiesOp
    | RemoveVertexPropertiesOp
    | AddVertexPropertiesOp
    | RenameVerticesOp
    | RenameRelationsOp
    | RenameResourcesOp
    | RemoveEdgesOp
    | MergeEdgesOp
    | RenameEdgePropertiesOp
    | RemoveEdgePropertiesOp
    | AddEdgePropertiesOp
    | DeclareEdgeInversesOp
    | RetractEdgeInversesOp
    | AddInverseEdgesOp
    | SetNativeInversesOp
    | SetInverseEmissionOp
    | ProjectManifestOp
    | ReplaceIdentityOp
    | SanitizeOp
    | MergeManifestsOp,
    PydanticField(discriminator="op"),
]


# Ops whose effect extends past `schema` into `ingestion_model`. Applying one to a
# manifest that carries no ingestion block silently drops that half of the work, which
# matters when schema and resources are stored as separate registry artifacts: the
# schema gains renamed vertices while the resources keep pointing at the old names.
# Every op in the vocabulary is classified — see
# ``test_evolution_codec.py::test_every_op_is_classified_for_ingestion_reach``.
INGESTION_REWRITING_OPS: frozenset[str] = frozenset(
    {
        "add_inverse_edges",
        "add_resource_transforms",
        "add_resources",
        "canonicalize",
        "remove_resources",
        "replace_resources",
        "ensure_extracted_fields",
        "merge_edges",
        "merge_vertices",
        "project_manifest",
        "remove_edge_properties",
        "remove_edges",
        "remove_vertex_properties",
        "remove_vertices",
        "rename_edge_properties",
        "rename_relations",
        "rename_resources",
        "rename_vertex_properties",
        "rename_vertices",
        "replace_identity",
        "retarget_edges",
        "sanitize",
        "set_inverse_emission",
    }
)


def ops_reaching_ingestion(ops: Sequence[Any]) -> list[str]:
    """Names of *ops* whose effect extends into ``ingestion_model``, in order."""
    return [
        name
        for name in (getattr(op, "op", None) for op in ops)
        if isinstance(name, str) and name in INGESTION_REWRITING_OPS
    ]
