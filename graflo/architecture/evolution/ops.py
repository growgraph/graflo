"""Typed manifest evolution operations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any, Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.architecture.graph_types import Index
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.identity_funnel import IdentityFunnel
from graflo.architecture.schema.vertex import FieldType, SecondaryIdentity, Vertex
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
            "both endpoints share one accumulator slot, so assembly merges rows that "
            "were previously separate nodes. Rejected unless set."
        ),
    )
    allow_row_fusion: bool = PydanticField(
        default=False,
        description=(
            "Accept resource pipelines that produce ``into`` more than once at the same "
            "level. Those steps then write to one accumulator slot, fusing what a single "
            "source document emitted as two entities. Rejected unless set."
        ),
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
    )


class RemoveVertexPropertiesOp(ConfigBaseModel):
    """Remove vertex properties and propagate pruning to ingestion/db profile references."""

    op: Literal["remove_vertex_properties"] = "remove_vertex_properties"
    removals: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-vertex field removal map: ``{vertex_name: [field_name, ...]}``."
        ),
    )


class AddVertexPropertiesOp(ConfigBaseModel):
    """Add vertex properties to existing logical vertex types."""

    op: Literal["add_vertex_properties"] = "add_vertex_properties"
    additions: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-vertex property additions: ``{vertex_name: [field_name, ...]}``."
        ),
    )


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
    vertices: dict[str, IdentityReplacement] = PydanticField(
        ...,
        description="Per-vertex identity replacement: ``{vertex_name: replacement}``.",
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
    vertices: dict[str, str] = PydanticField(
        ...,
        description="Vertex rename map: ``{old_vertex: new_vertex}``. Must be injective.",
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameVerticesOp:
        validate_rename_map_is_injective(
            self.vertices,
            kind="rename_vertices",
            merge_hint="MergeVerticesOp(sources=[...], into=...)",
        )
        return self


class RenameRelationsOp(ConfigBaseModel):
    """Rename logical edge relation names across schema and ingestion."""

    op: Literal["rename_relations"] = "rename_relations"
    relations: dict[str, str] = PydanticField(
        ...,
        description="Relation rename map: ``{old_relation: new_relation}``. Must be injective.",
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameRelationsOp:
        validate_rename_map_is_injective(
            self.relations,
            kind="rename_relations",
            merge_hint="MergeEdgesOp(sources=[...], into=...)",
        )
        return self


class RenameResourcesOp(ConfigBaseModel):
    """Rename ingestion resource names and bindings references."""

    op: Literal["rename_resources"] = "rename_resources"
    resources: dict[str, str] = PydanticField(
        ...,
        description=(
            "Ingestion resource rename map: ``{old_resource: new_resource}``. Must be injective."
        ),
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameResourcesOp:
        # IngestionModel already rejects duplicate resource names, so a collapsing
        # map fails downstream anyway — but with a message about the model rather
        # than about the op the author actually wrote.
        validate_rename_map_is_injective(
            self.resources,
            kind="rename_resources",
            merge_hint="ComposeManifestsOp with explicit resource_renames",
        )
        return self


class RemoveEdgesOp(ConfigBaseModel):
    """Remove logical edge relations from schema, profile, and ingestion selectors."""

    op: Literal["remove_edges"] = "remove_edges"
    relations: list[str] = PydanticField(
        ...,
        description="Relation names to remove from edge definitions and references.",
        min_length=1,
    )


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


class RenameEdgePropertiesOp(ConfigBaseModel):
    """Rename edge properties for each relation across schema/profile/ingestion."""

    op: Literal["rename_edge_properties"] = "rename_edge_properties"
    renames: dict[str, dict[str, str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field rename map: "
            "``{relation_name: {old_field: new_field}}``."
        ),
    )


class RemoveEdgePropertiesOp(ConfigBaseModel):
    """Remove edge properties for each relation across schema/profile/ingestion."""

    op: Literal["remove_edge_properties"] = "remove_edge_properties"
    removals: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field removals: ``{relation_name: [field_name, ...]}``."
        ),
    )


class AddEdgePropertiesOp(ConfigBaseModel):
    """Add edge properties for each relation in schema/profile defaults."""

    op: Literal["add_edge_properties"] = "add_edge_properties"
    additions: dict[str, list[str]] = PydanticField(
        ...,
        description=(
            "Per-relation edge field additions: ``{relation_name: [field_name, ...]}``."
        ),
    )


class AddInverseEdgesOp(ConfigBaseModel):
    """Add inverse edge relations for matching relations across schema and ingestion."""

    op: Literal["add_inverse_edges"] = "add_inverse_edges"
    relations: dict[str, str] = PydanticField(
        ...,
        description=(
            "Relation inverse map: ``{relation_name: inverse_relation_name}``."
        ),
    )


class AddResourceTransformsOp(ConfigBaseModel):
    """Append transform steps to named resources' pipelines.

    The first op whose primary effect is ingestion: ``graph_schema`` is
    untouched. Steps are appended at the ROOT level of each pipeline; actor
    type-priority sorting (transform runs before vertex extraction at the same
    level) makes the position safe. Nested ``descend`` levels are not
    targetable in this form.

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
        for proto in self.transforms:
            if not proto.name:
                raise ValueError(
                    "add_resource_transforms: registry transforms must define "
                    "a non-empty name"
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
    indexes: dict[str, list[Index]] = PydanticField(
        ...,
        description="``{vertex_name: [Index, ...]}``.",
        min_length=1,
    )


class RemoveVertexIndexesOp(ConfigBaseModel):
    """Withdraw authored vertex indexes, addressed by field list.

    Indexes derived from ``secondary_identities`` are not removable here — they would
    be re-registered by the next ``finish_init``. Use
    :class:`RemoveSecondaryIdentitiesOp` for those.
    """

    op: Literal["remove_vertex_indexes"] = "remove_vertex_indexes"
    indexes: dict[str, list[list[str]]] = PydanticField(
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


class AddEdgeIndexesOp(ConfigBaseModel):
    """Author secondary indexes on edge physical specs."""

    op: Literal["add_edge_indexes"] = "add_edge_indexes"
    edges: list[EdgeIndexEntry] = PydanticField(
        ...,
        description="Per-spec indexes to add (``indexes`` on each entry).",
        min_length=1,
    )


class RemoveEdgeIndexesOp(ConfigBaseModel):
    """Withdraw authored indexes from edge physical specs, addressed by field list."""

    op: Literal["remove_edge_indexes"] = "remove_edge_indexes"
    edges: list[EdgeIndexEntry] = PydanticField(
        ...,
        description="Per-spec indexes to remove (``fields`` on each entry).",
        min_length=1,
    )


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


class PropertyEquivalence(ConfigBaseModel):
    """Align a property from the left and/or right vertex onto a canonical name.

    At least one of ``left`` / ``right`` must be set. When both are set, both fields
    rename to ``into`` before the vertices are merged.
    """

    left: str | None = PydanticField(
        default=None,
        description="Field name on the left vertex (omit to keep right-only).",
    )
    right: str | None = PydanticField(
        default=None,
        description="Field name on the right vertex (omit to keep left-only).",
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
        return self


class VertexEquivalence(ConfigBaseModel):
    """Collapse a left vertex and a right vertex into one composed type.

    GraFlo applies this map deterministically; it does not infer semantic matches.
    """

    left: str = PydanticField(..., description="Vertex type name in the left manifest.")
    right: str = PydanticField(
        ..., description="Vertex type name in the right manifest."
    )
    into: str = PydanticField(
        ...,
        description=(
            "Canonical vertex type name after compose "
            "(may equal ``left``, ``right``, or a new name)."
        ),
    )
    properties: list[PropertyEquivalence] = PydanticField(
        default_factory=list,
        description="Property alignment map applied before the vertex merge.",
    )
    identity: list[str] | None = PydanticField(
        default=None,
        description=(
            "Optional explicit natural-key identity on ``into`` (property names after "
            "alignment). When unset, identity is the merged union of both sides, "
            "with any ``PropertyEquivalence.identity`` flags appended."
        ),
    )


class RelationEquivalence(ConfigBaseModel):
    """Collapse a left relation and a right relation onto one canonical name."""

    left: str = PydanticField(..., description="Relation name in the left manifest.")
    right: str = PydanticField(..., description="Relation name in the right manifest.")
    into: str = PydanticField(..., description="Canonical relation name after compose.")


class ComposeManifestsOp(ConfigBaseModel):
    """Union two full ``GraphManifest``s using explicit equivalence maps.

    Binary only — apply via :func:`~graflo.architecture.evolution.compose.compose_manifests`.
    Unary :func:`~graflo.architecture.evolution.apply.apply_evolution` rejects this op.

    Empty ``vertices`` / ``relations`` yields a disjoint union (schema + resources +
    bindings), subject to ``name_conflict`` / ``resource_renames``.
    """

    op: Literal["compose_manifests"] = "compose_manifests"
    vertices: list[VertexEquivalence] = PydanticField(
        default_factory=list,
        description="Explicit vertex equivalences across the two input manifests.",
    )
    relations: list[RelationEquivalence] = PydanticField(
        default_factory=list,
        description="Optional relation equivalences across the two input manifests.",
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


ManifestOp = Annotated[
    RemoveVerticesOp
    | AddResourceTransformsOp
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
        "merge_edges",
        "merge_vertices",
        "project_manifest",
        "remove_edges",
        "remove_vertex_properties",
        "remove_vertices",
        "rename_relations",
        "rename_resources",
        "rename_vertex_properties",
        "rename_vertices",
        "replace_identity",
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
