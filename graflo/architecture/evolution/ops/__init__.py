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

from collections.abc import Sequence
from typing import Annotated, Any

from pydantic import Field as PydanticField

from .branches import (
    DerivationSpec,
    DerivedBranch,
    IdentityBranchDecl,
    LocalKeyBranch,
    LocalKeySource,
    RawBranch,
    branch_fields,
    branch_id,
    check_identity_branches,
    identity_branches_funnel,
)
from .canonical import CanonicalizeOp, CanonicalMap
from .edges import (
    AddEdgePropertiesOp,
    AddEdgesOp,
    EdgeRetargetEntry,
    EdgeSelector,
    MergeEdgesOp,
    RemoveEdgePropertiesOp,
    RemoveEdgesOp,
    RenameEdgePropertiesOp,
    RenameRelationsOp,
    RetargetEdgesOp,
    SetEdgeDirectedOp,
)
from .equivalence import PropertyEquivalence, RelationEquivalence, VertexEquivalence
from .field_types import ChangeFieldTypesOp, FieldTypeSpec, MergeFieldTypes
from .identities import (
    AddSecondaryIdentitiesOp,
    AssignedIdentityTarget,
    BlankIdentityTarget,
    EdgeIdentitiesEntry,
    FunnelIdentityTarget,
    HashIdentityTarget,
    IdentityReplacement,
    IdentityTarget,
    NaturalIdentityTarget,
    RemoveSecondaryIdentitiesOp,
    ReplaceEdgeIdentitiesOp,
    ReplaceIdentityOp,
)
from .indexes import (
    AddEdgeIndexesOp,
    AddVertexIndexesOp,
    EdgeIndexEntry,
    RemoveEdgeIndexesOp,
    RemoveVertexIndexesOp,
)
from .inverses import (
    AddInverseEdgesOp,
    DeclareEdgeInversesOp,
    RetractEdgeInversesOp,
    SetInverseEmissionOp,
    SetNativeInversesOp,
)
from .manifest import ProjectManifestOp, SanitizeOp, SetBindingsOp, SetDbProfileOp
from .merge import MergeManifestsOp, MergeRenames, SideRenames
from .resources import (
    AddResourcesOp,
    AddResourceTransformsOp,
    EnsureExtractedFields,
    EnsureExtractedFieldsOp,
    RemoveResourcesOp,
    RenameResourcesOp,
    ReplaceResourcesOp,
)
from .semantics import (
    EdgeFieldSemanticsTarget,
    FieldSemanticsTarget,
    SetEdgeSemanticsOp,
    SetFieldSemanticsOp,
    SetVertexDescriptionsOp,
    SetVertexSemanticsOp,
)
from .validation import (
    validate_merge_sources,
    validate_rename_map_is_injective,
    validate_vocabulary_is_idempotent,
    validate_vocabulary_map,
    vocabulary_groups,
)
from .vertices import (
    AddVertexPropertiesOp,
    AddVerticesOp,
    MergeVerticesOp,
    RemoveVertexPropertiesOp,
    RemoveVerticesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
)

__all__ = [
    "INGESTION_REWRITING_OPS",
    "AddEdgeIndexesOp",
    "AddEdgePropertiesOp",
    "AddEdgesOp",
    "AddInverseEdgesOp",
    "AddResourceTransformsOp",
    "AddResourcesOp",
    "AddSecondaryIdentitiesOp",
    "AddVertexIndexesOp",
    "AddVertexPropertiesOp",
    "AddVerticesOp",
    "AssignedIdentityTarget",
    "BlankIdentityTarget",
    "CanonicalMap",
    "CanonicalizeOp",
    "ChangeFieldTypesOp",
    "DeclareEdgeInversesOp",
    "DerivationSpec",
    "DerivedBranch",
    "EdgeFieldSemanticsTarget",
    "EdgeIdentitiesEntry",
    "EdgeIndexEntry",
    "EdgeRetargetEntry",
    "EdgeSelector",
    "EnsureExtractedFields",
    "EnsureExtractedFieldsOp",
    "FieldSemanticsTarget",
    "FieldTypeSpec",
    "FunnelIdentityTarget",
    "HashIdentityTarget",
    "IdentityBranchDecl",
    "IdentityReplacement",
    "IdentityTarget",
    "LocalKeyBranch",
    "LocalKeySource",
    "ManifestOp",
    "MergeEdgesOp",
    "MergeFieldTypes",
    "MergeManifestsOp",
    "MergeRenames",
    "MergeVerticesOp",
    "NaturalIdentityTarget",
    "ProjectManifestOp",
    "PropertyEquivalence",
    "RawBranch",
    "RelationEquivalence",
    "RemoveEdgeIndexesOp",
    "RemoveEdgePropertiesOp",
    "RemoveEdgesOp",
    "RemoveResourcesOp",
    "RemoveSecondaryIdentitiesOp",
    "RemoveVertexIndexesOp",
    "RemoveVertexPropertiesOp",
    "RemoveVerticesOp",
    "RenameEdgePropertiesOp",
    "RenameRelationsOp",
    "RenameResourcesOp",
    "RenameVertexPropertiesOp",
    "RenameVerticesOp",
    "ReplaceEdgeIdentitiesOp",
    "ReplaceIdentityOp",
    "ReplaceResourcesOp",
    "RetargetEdgesOp",
    "RetractEdgeInversesOp",
    "SanitizeOp",
    "SetBindingsOp",
    "SetDbProfileOp",
    "SetEdgeDirectedOp",
    "SetEdgeSemanticsOp",
    "SetFieldSemanticsOp",
    "SetInverseEmissionOp",
    "SetNativeInversesOp",
    "SetVertexDescriptionsOp",
    "SetVertexSemanticsOp",
    "SideRenames",
    "VertexEquivalence",
    "branch_fields",
    "branch_id",
    "check_identity_branches",
    "identity_branches_funnel",
    "ops_reaching_ingestion",
    "validate_merge_sources",
    "validate_rename_map_is_injective",
    "validate_vocabulary_is_idempotent",
    "validate_vocabulary_map",
    "vocabulary_groups",
]


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
