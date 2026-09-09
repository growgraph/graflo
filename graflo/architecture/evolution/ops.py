"""Typed manifest evolution operations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, field_validator, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.architecture.graph_types import Index
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.identity_funnel import IdentityFunnel
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
            "Accept resource pipelines that produce ``into`` more than once at the same "
            "level. Those steps then write to one accumulator slot, fusing into one node "
            "what a single source document emitted as two vertex observations. Rejected "
            "unless set. ``allow_row_fusion`` is accepted as a legacy alias."
        ),
    )

    @model_validator(mode="after")
    def _validate_sources(self) -> MergeVerticesOp:
        validate_merge_sources(self.sources, self.into, kind="merge_vertices")
        return self


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
    secondary identities at all), and a no-op replacement does not bump the version.
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
            merge_hint="ComposeManifestsOp with explicit resource_renames",
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


class AddInverseEdgesOp(ConfigBaseModel):
    """Add inverse edge relations for matching relations across schema and ingestion."""

    op: Literal["add_inverse_edges"] = "add_inverse_edges"
    inverses: dict[str, str] = PydanticField(
        ...,
        validation_alias=AliasChoices("inverses", "relations"),
        description=(
            "Relation inverse map: ``{relation_name: inverse_relation_name}``. "
            "Must be injective, and no relation may be its own inverse. "
            "``relations`` is accepted as a legacy alias."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> AddInverseEdgesOp:
        # Two relations sharing one inverse name would create two edge types
        # under it -- the collapse the rename ops refuse, reached sideways.
        validate_rename_map_is_injective(
            self.inverses,
            kind="add_inverse_edges",
            merge_hint="one inverse relation name per source relation",
        )
        self_inverse = sorted(r for r, inv in self.inverses.items() if r == inv)
        if self_inverse:
            raise ValueError(
                f"add_inverse_edges: a relation cannot be its own inverse: {self_inverse}"
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

    The unary counterpart to what :class:`ComposeManifestsOp` can only do binarily.
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


class ProjectManifestOp(ConfigBaseModel):
    """Project a manifest to a vertex/edge subgraph with consistent cascade.

    Keeps only the requested logical vertices and edges (and optionally resources).
    All schema, ``db_profile``, ingestion, and bindings references to removed
    entities are pruned. Inverse edges are **not** auto-kept; list them explicitly
    in ``keep_edges`` when needed.

    With ``connectivity=\"induced_prune\"`` (v1 default), when ``keep_vertices`` is
    set, vertex types from that list with no incident surviving edge are dropped.
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
    keep_resources: list[str] | None = PydanticField(
        default=None,
        description="Optional ingestion resource names to retain after graph slice.",
    )
    strict: bool = PydanticField(
        default=True,
        description="When True, unknown vertex/edge selectors raise ``ValueError``.",
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
        return self


class SanitizeOp(ConfigBaseModel):
    """Apply DB-flavor-specific name/field sanitization to a manifest.

    Composes (in order):

    1. Storage-name sanitization on ``DatabaseProfile`` (vertex storage names + edge
       relation names) against the flavor's reserved-words set.
    2. Vertex field rename for fields whose names are reserved words.
    3. For TigerGraph, normalize identity fields across edges that share a relation
       (TigerGraph requires consistent source/target indexes per relation).
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
        default="gated_normalized_key",
        description="Function name; called as ``foo(*values, **params)``.",
    )
    params: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Keyword parameters for the function (gate prefix, ...).",
    )


class SharedDerivation(ConfigBaseModel):
    """One derivation shared by several members, varying only in parameters.

    The compact spelling of the member-keyed form for the common case: the
    call is the same for every member and only a parameter changes — a marker
    prefix per class — or nothing does. ``members`` is a list of member
    classes, or a dict from member to the parameters that differ; each
    member's derivation is ``spec`` with those parameters laid over
    ``spec.params``. Anything else that differs between members — the input
    columns, the function — is a different derivation: spell it with the
    explicit ``{member: spec}`` dict.

    Expands to that dict; the lowering never sees this model.
    """

    spec: DerivationSpec = PydanticField(
        ...,
        description="The derivation every member shares.",
    )
    members: list[str] | dict[str, dict[str, Any]] = PydanticField(
        ...,
        description=(
            "Member classes sharing ``spec``: a list when nothing varies, or "
            "``{member: {param: value}}`` naming what does."
        ),
    )

    @model_validator(mode="after")
    def _validate_members(self) -> SharedDerivation:
        if not self.members:
            raise ValueError("SharedDerivation: members must name at least one class")
        if isinstance(self.members, list) and len(set(self.members)) != len(
            self.members
        ):
            raise ValueError("SharedDerivation: members lists a class twice")
        return self

    def expand(self) -> dict[str, DerivationSpec]:
        """The explicit ``{member: spec}`` dict this stands for."""
        if isinstance(self.members, list):
            return {member: self.spec for member in self.members}
        return {
            member: self.spec.model_copy(
                update={"params": {**self.spec.params, **overrides}}
            )
            for member, overrides in self.members.items()
        }


class AlignmentAttribute(ConfigBaseModel):
    """One aligned canonical attribute; list position = funnel priority.

    Each entry lowers to one
    :class:`~graflo.architecture.schema.identity_funnel.IdentityBranch` over
    ``into``, so the list order *is* the funnel order.

    ``sources`` is keyed by resource because derivation inputs are that
    resource's raw column names. An entry takes one of three shapes:

    * a single :class:`DerivationSpec` — the resource produces one member of
      the cluster, or all its members share the key column;
    * a **list** of specs — the resource produces several members, each with
      its **own key column**, and at most one spec yields a value for any
      document (the others read an empty column). Lowers to scratch fields
      plus a ``coalesce_fields`` step;
    * a **dict keyed by member class** — the resource produces several members
      and which one a document *is* must decide the derivation (the members
      share a column, or each carries its own marker). The lowering asks the
      side manifest how the resource produces each member and guards the step
      accordingly (``when`` on the router's discriminator, or nothing for a
      plain ``vertex`` step). Member names are the classes the
      :class:`VertexEquivalence` names on that side, after canonical maps;
    * a :class:`SharedDerivation` — the same dict, spelled once: one call
      shared by the listed members, with only the parameters that differ.
    """

    into: str = PydanticField(
        ...,
        description="Canonical attribute name on the class; funnel branch id.",
    )
    sources: dict[
        str,
        DerivationSpec
        | SharedDerivation
        | list[DerivationSpec]
        | dict[str, DerivationSpec],
    ] = PydanticField(
        ...,
        min_length=1,
        description=(
            "Per-resource derivation: ``{resource: spec}``; ``{resource: "
            "[spec, ...]}`` when the members the resource produces carry "
            "different key columns; ``{resource: {member_class: spec}}`` when "
            "the member a document is must decide the derivation, or a "
            "``SharedDerivation`` spelling that dict once."
        ),
    )

    def _by_member(self, resource: str) -> dict[str, DerivationSpec] | None:
        spec = self.sources.get(resource)
        if isinstance(spec, SharedDerivation):
            return spec.expand()
        return spec if isinstance(spec, dict) else None

    def specs_for(self, resource: str) -> list[DerivationSpec]:
        """Derivations *resource* contributes to this attribute, in order."""
        spec = self.sources.get(resource)
        if spec is None:
            return []
        if isinstance(spec, DerivationSpec):
            return [spec]
        if isinstance(spec, list):
            return list(spec)
        by_member = spec.expand() if isinstance(spec, SharedDerivation) else spec
        return list(by_member.values())

    def members_for(self, resource: str) -> list[str] | None:
        """Member classes keying *resource*'s specs, or ``None`` if unkeyed."""
        by_member = self._by_member(resource)
        return list(by_member) if by_member is not None else None


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

    @field_validator("tag", mode="before")
    @classmethod
    def _none_is_the_empty_tag(cls, value: Any) -> Any:
        return "" if value is None else value

    gate: str | None = PydanticField(
        default=None,
        description=(
            "Optional RAW doc field deciding whether this source applies — the "
            "router's discriminator when one resource contributes several "
            "local keys. Omit when ``field`` is empty for the other branches, "
            "which already selects."
        ),
    )
    gate_prefix: str = PydanticField(
        default="",
        description=(
            'Required prefix of the ``gate`` value; ``""`` always passes. '
            "Meaningless without ``gate``."
        ),
    )

    @model_validator(mode="after")
    def _validate_gate(self) -> LocalKeySource:
        if self.gate is None and self.gate_prefix:
            raise ValueError(
                "LocalKeySource: gate_prefix is meaningless without a gate field"
            )
        return self


class LocalKeySpec(ConfigBaseModel):
    """The canonical fallback identity attribute for non-aligned records.

    ``sources`` takes the same three shapes as
    :attr:`AlignmentAttribute.sources`: one source, a list (one per member,
    each reading its own column), or a dict keyed by member class (the member
    decides; the gate is derived from how the resource produces it, so a
    member-keyed source must not set ``gate``).
    """

    into: str = PydanticField(
        default="local_key",
        description="Canonical fallback property name on the class.",
    )
    sep: str = PydanticField(
        default=":",
        description="Separator between tag and key.",
    )
    sources: dict[
        str, LocalKeySource | list[LocalKeySource] | dict[str, LocalKeySource]
    ] = PydanticField(
        ...,
        min_length=1,
        description=(
            "Per-resource local-key wiring: ``{resource: source}``; ``{resource: "
            "[source, ...]}`` when the members carry different key columns; "
            "``{resource: {member_class: source}}`` when the member decides."
        ),
    )

    @model_validator(mode="after")
    def _validate_member_sources(self) -> LocalKeySpec:
        for resource, entry in self.sources.items():
            if not isinstance(entry, dict):
                continue
            gated = sorted(m for m, src in entry.items() if src.gate is not None)
            if gated:
                raise ValueError(
                    f"LocalKeySpec: member-keyed sources for resource {resource!r} "
                    f"set a gate on {gated}; the member already decides, and the "
                    "gate is derived from how the resource produces it"
                )
        return self

    def sources_for(self, resource: str) -> list[LocalKeySource]:
        """Local-key sources *resource* contributes, in order."""
        source = self.sources.get(resource)
        if source is None:
            return []
        if isinstance(source, LocalKeySource):
            return [source]
        return list(source.values()) if isinstance(source, dict) else list(source)

    def members_for(self, resource: str) -> list[str] | None:
        """Member classes keying *resource*'s sources, or ``None`` if unkeyed."""
        source = self.sources.get(resource)
        return list(source) if isinstance(source, dict) else None


class IdentityAlignment(ConfigBaseModel):
    """Cross-source identity alignment for one canonical class.

    ``attributes`` order is funnel priority: a record keys by the highest-priority
    aligned attribute it carries. Two records fuse when their strongest
    present attribute coincides — a match on a lower-priority attribute does
    NOT fuse records when one of them also carries a higher-priority one.
    """

    vertex: str = PydanticField(
        ...,
        description="The canonical class whose identity is being aligned.",
    )
    attributes: list[AlignmentAttribute] = PydanticField(
        default_factory=list,
        validation_alias=AliasChoices("attributes", "rows"),
        description=(
            "Aligned canonical attributes, in priority order. ``rows`` is "
            "accepted as a legacy alias."
        ),
    )
    local_key: LocalKeySpec | None = PydanticField(
        default=None,
        description=(
            "Fallback identity for records carrying no aligned attribute. "
            "Without it such records get no identity and are dropped."
        ),
    )
    secondary_identities: dict[str, list[str]] = PydanticField(
        default_factory=dict,
        description=(
            "Retired side keys kept as lookup-only secondary identities: "
            "``{name: [field, ...]}``."
        ),
    )
    at: dict[str, list[int]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-resource pipeline level to derive at, as ``descend`` step "
            "indices. Omitted resources resolve to the single level producing "
            "``vertex`` (for member-keyed sources: the level producing the "
            "member on its side); supply a path only when a resource produces "
            "it at more than one level."
        ),
    )

    @model_validator(mode="after")
    def _validate_shape(self) -> IdentityAlignment:
        if not self.attributes and self.local_key is None:
            raise ValueError(
                "IdentityAlignment requires at least one attribute or a local_key"
            )
        into_names = [attribute.into for attribute in self.attributes]
        if self.local_key is not None:
            into_names.append(self.local_key.into)
        duplicates = {n for n in into_names if into_names.count(n) > 1}
        if duplicates:
            raise ValueError(
                f"IdentityAlignment: duplicate target attributes {sorted(duplicates)}"
            )
        return self


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
    Declare an equivalence only to rename, to pick a different ``into``, or to
    flag ``identity=True``.
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
        description="Canonical property name on the composed vertex.",
    )
    identity: bool = PydanticField(
        default=False,
        description=(
            "When True and ``VertexEquivalence.identity`` is unset, include ``into`` "
            "in the derived identity list after merge."
        ),
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


IdentityBranchSpec = str | list[str]


class SideIdentity(ConfigBaseModel):
    """Per-side/per-member shorthand for a cluster's composed identity funnel.

    Each entry is one funnel branch: a single canonical attribute, or an
    ordered composite (``list[str]``). ``left`` / ``right`` supply the default
    branch chain for every member declared on that side; ``members`` overrides
    it for specific member classes, keyed by the member's own (pre-canonical)
    name. Every chain is merged into one global branch order — see
    :func:`~graflo.architecture.evolution.compose.side_identity_to_funnel` —
    so declaring the same relative order on every member is required; two
    members disagreeing on the order of two branches raises.
    """

    left: list[IdentityBranchSpec] | None = PydanticField(
        default=None,
        description="Default ordered branch chain for every left member.",
    )
    right: list[IdentityBranchSpec] | None = PydanticField(
        default=None,
        description="Default ordered branch chain for every right member.",
    )
    members: dict[str, list[IdentityBranchSpec]] = PydanticField(
        default_factory=dict,
        description="Per-member branch chain, overriding the side default.",
    )

    @model_validator(mode="after")
    def _validate_shape(self) -> SideIdentity:
        if self.left is None and self.right is None and not self.members:
            raise ValueError(
                "SideIdentity requires at least one of left, right, or members"
            )
        return self


class VertexEquivalence(ConfigBaseModel):
    """Collapse one or more left classes and one or more right classes into one.

    GraFlo applies this map deterministically; it does not infer semantic
    matches. ``left`` / ``right`` accept a bare class name (a 1-1 equivalence)
    or a list (an n-ary cluster): ``{Company, Shop} ~ {Org, Branch} ->
    Company``. Declaring more than one member on a side is a merge and
    requires ``ComposeManifestsOp.allow_merges=True``.

    Properties with the same spelling on every member after alignment fuse by
    exact name without an entry in ``properties`` — list only renames and
    identity-flagged fields.
    """

    left: str | list[str] = PydanticField(
        ..., description="One or more left-manifest vertex type names."
    )
    right: str | list[str] = PydanticField(
        ..., description="One or more right-manifest vertex type names."
    )
    into: str = PydanticField(
        ...,
        description=(
            "Canonical vertex type name after compose "
            "(may equal a member's name, or a new name)."
        ),
    )
    properties: list[PropertyEquivalence] = PydanticField(
        default_factory=list,
        description="Property alignment map applied before the vertex merge.",
    )
    identity: list[str] | IdentityFunnel | SideIdentity | None = PydanticField(
        default=None,
        description=(
            "Optional explicit composed identity, in canonical attribute names "
            "(after alignment): a natural key, an explicit funnel, or a "
            "`SideIdentity` shorthand lowered to one funnel. When unset, "
            "identity is carried through only if every member agrees after "
            "alignment (plus any `PropertyEquivalence.identity` flags); "
            "disagreement with nothing declared raises `ComposeIdentityError`."
        ),
    )
    retire: Literal["demote", "keep"] = PydanticField(
        default="demote",
        description=(
            "What becomes of each member's pre-merge identity fields when "
            "`identity` is declared. `demote` keeps them as lookup-only "
            "secondary identities on `into`; `keep` drops them. Unused when "
            "`identity` is unset."
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
        for pe in self.properties:
            for side, spec in (("left", pe.left), ("right", pe.right)):
                if isinstance(spec, dict):
                    unknown = sorted(set(spec) - set(self.members(side)))
                    if unknown:
                        raise ValueError(
                            f"VertexEquivalence: property equivalence into "
                            f"{pe.into!r} names {side} member(s) {unknown} not "
                            f"in {side}={self.members(side)}"
                        )
        if self.identity is not None:
            flagged = [pe.into for pe in self.properties if pe.identity]
            if flagged:
                raise ValueError(
                    "VertexEquivalence: `identity` is declared on the cluster; "
                    f"PropertyEquivalence.identity=True on {flagged} is "
                    "redundant and conflicting — declare the composed key one "
                    "way, not both"
                )
        return self


class RelationEquivalence(ConfigBaseModel):
    """Collapse one or more left relations and one or more right relations onto one name.

    Shares the ``left`` / ``right`` n-ary shape of :class:`VertexEquivalence`:
    a bare name is a 1-1 equivalence, a list is a merge and requires
    ``ComposeManifestsOp.allow_merges=True``.
    """

    left: str | list[str] = PydanticField(
        ..., description="One or more left relation names."
    )
    right: str | list[str] = PydanticField(
        ..., description="One or more right relation names."
    )
    into: str = PydanticField(..., description="Canonical relation name after compose.")

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


class ComposeManifestsOp(ConfigBaseModel):
    """Union two full ``GraphManifest``s using explicit equivalence maps.

    Binary only — apply via :func:`~graflo.architecture.evolution.compose.compose_manifests`.
    Unary :func:`~graflo.architecture.evolution.apply.apply_evolution` rejects this op.

    Empty ``vertex_equivalences`` / ``relation_equivalences`` yields a disjoint
    union (schema + resources + bindings), subject to ``name_conflict`` /
    ``resource_renames``.

    ``identity_alignments`` are applied to the composed union before return
    (canonical attributes → resource derivations → priority funnel → secondaries).
    Each entry's ``vertex`` must be a declared cluster's ``into`` label.
    """

    op: Literal["compose_manifests"] = "compose_manifests"
    vertex_equivalences: list[VertexEquivalence] = PydanticField(
        default_factory=list,
        validation_alias=AliasChoices("vertex_equivalences", "vertices"),
        description=(
            "Explicit vertex equivalences across the two input manifests. "
            "``vertices`` is accepted as a legacy alias."
        ),
    )
    relation_equivalences: list[RelationEquivalence] = PydanticField(
        default_factory=list,
        validation_alias=AliasChoices("relation_equivalences", "relations"),
        description=(
            "Optional relation equivalences across the two input manifests. "
            "``relations`` is accepted as a legacy alias."
        ),
    )
    resource_renames: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Rename map applied to *right* resource names before union.",
    )
    name_conflict: Literal["error", "prefix_right", "fuse_right"] = PydanticField(
        default="error",
        description=(
            "How to handle non-equivalent name collisions on the right side "
            "(vertices, relations, resources, connectors). Vertex and relation "
            "names collide both exactly and when they key alike under "
            "``canonical_key`` -- ``OrderLine`` and ``order_line`` are one "
            "concept spelled two ways, and composing them into two unrelated "
            "types splits the data silently. ``prefix_right`` prefixes "
            "colliding names with ``r_``; ``fuse_right`` adopts the left "
            "spelling for a canonical near-collision, and applies to vertices "
            "and relations only (resources and connectors are addresses, not "
            "concepts, so it behaves as ``error`` for them)."
        ),
    )
    allow_merges: bool = PydanticField(
        default=False,
        description=(
            "Accept a vertex or relation equivalence that collapses more than "
            "one class/relation on a side. A merge is a stated intent — it "
            "fuses entities and can create self-relations — so it must be "
            "acknowledged here rather than inferred from the equivalence list."
        ),
    )
    allow_self_relations: bool = PydanticField(
        default=False,
        description=(
            "Accept a merge whose sources are connected by an edge that "
            "becomes a self-relation once both endpoints land on the same "
            "composed vertex. Forwarded to the per-side ``MergeVerticesOp``."
        ),
    )
    allow_observation_fusion: bool = PydanticField(
        default=False,
        validation_alias=AliasChoices("allow_observation_fusion", "allow_row_fusion"),
        description=(
            "Accept a merge whose sources are produced more than once at one "
            "resource pipeline level. Forwarded to the per-side "
            "``MergeVerticesOp``. ``allow_row_fusion`` is accepted as a legacy alias."
        ),
    )
    identity_alignments: list[IdentityAlignment] = PydanticField(
        default_factory=list,
        description=(
            "Optional identity alignments applied after the schema/resource "
            "union, one per composed class."
        ),
    )

    @model_validator(mode="after")
    def _require_allow_merges_for_nary(self) -> ComposeManifestsOp:
        if self.allow_merges:
            return self
        offending: set[str] = set()
        for veq in self.vertex_equivalences:
            if len(veq.left_members) > 1 or len(veq.right_members) > 1:
                offending.add(f"vertex equivalence into {veq.into!r}")
        for req in self.relation_equivalences:
            if len(req.left_members) > 1 or len(req.right_members) > 1:
                offending.add(f"relation equivalence into {req.into!r}")
        if offending:
            raise ValueError(
                "compose_manifests: "
                + "; ".join(sorted(offending))
                + " collapses more than one class/relation on a side; a merge "
                "is a stated intent — set allow_merges=True"
            )
        return self


ManifestOp = Annotated[
    RemoveVerticesOp
    | AddResourceTransformsOp
    | EnsureExtractedFieldsOp
    | AddResourcesOp
    | RemoveResourcesOp
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
    | SetVertexSemanticsOp
    | SetEdgeSemanticsOp
    | SetFieldSemanticsOp
    | MergeVerticesOp
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
    | AddInverseEdgesOp
    | ProjectManifestOp
    | ReplaceIdentityOp
    | SanitizeOp
    | ComposeManifestsOp,
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
        "remove_resources",
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
    }
)


def ops_reaching_ingestion(ops: Sequence[Any]) -> list[str]:
    """Names of *ops* whose effect extends into ``ingestion_model``, in order."""
    return [
        name
        for name in (getattr(op, "op", None) for op in ops)
        if isinstance(name, str) and name in INGESTION_REWRITING_OPS
    ]
