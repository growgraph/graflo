"""Manifest evolution: apply high-level schema + ingestion transforms to :class:`~graflo.architecture.contract.manifest.GraphManifest`.

Use :func:`~graflo.migrate.io.manifest_hash` to compare contract identity before and after.
"""

from __future__ import annotations

from typing import Any

from .ops import (
    INGESTION_REWRITING_OPS,
    AddEdgeIndexesOp,
    AddEdgePropertiesOp,
    AddEdgesOp,
    AddInverseEdgesOp,
    AddResourcesOp,
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexIndexesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    AssignedIdentityTarget,
    BlankIdentityTarget,
    CanonicalizeOp,
    ChangeFieldTypesOp,
    DeclareEdgeInversesOp,
    EdgeFieldSemanticsTarget,
    EdgeIdentitiesEntry,
    EdgeIndexEntry,
    EdgeRetargetEntry,
    EdgeSelector,
    EnsureExtractedFields,
    EnsureExtractedFieldsOp,
    FieldSemanticsTarget,
    FieldTypeSpec,
    FunnelIdentityTarget,
    HashIdentityTarget,
    IdentityReplacement,
    IdentityTarget,
    ManifestOp,
    MergeEdgesOp,
    MergeManifestsOp,
    MergeVerticesOp,
    NaturalIdentityTarget,
    ProjectManifestOp,
    PropertyEquivalence,
    RelationEquivalence,
    RemoveEdgeIndexesOp,
    RemoveEdgePropertiesOp,
    RemoveEdgesOp,
    RemoveResourcesOp,
    RemoveSecondaryIdentitiesOp,
    RemoveVertexIndexesOp,
    RemoveVertexPropertiesOp,
    RemoveVerticesOp,
    RenameEdgePropertiesOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    ReplaceEdgeIdentitiesOp,
    ReplaceIdentityOp,
    RetargetEdgesOp,
    RetractEdgeInversesOp,
    SanitizeOp,
    SetEdgeDirectedOp,
    SetEdgeSemanticsOp,
    SetFieldSemanticsOp,
    SetInverseEmissionOp,
    SetNativeInversesOp,
    SetVertexDescriptionsOp,
    SetVertexSemanticsOp,
    SideIdentity,
    VertexEquivalence,
    ops_reaching_ingestion,
)

_APPLY_EXPORTS = frozenset(
    {
        "apply_ensure_extracted_fields",
        "apply_evolution",
        "apply_add_edge_properties",
        "apply_add_inverse_edges",
        "apply_add_vertex_properties",
        "apply_canonicalize",
        "apply_declare_edge_inverses",
        "apply_merge_edges",
        "apply_merge_vertices",
        "apply_project_manifest",
        "apply_remove_edge_properties",
        "apply_remove_edges",
        "apply_remove_edge_ids",
        "apply_remove_vertex_properties",
        "apply_remove_vertices",
        "apply_rename_edge_properties",
        "apply_rename_relations",
        "apply_rename_resources",
        "apply_rename_vertex_properties",
        "apply_rename_vertices",
        "apply_retract_edge_inverses",
        "apply_sanitize",
    }
)

_MERGE_EXPORTS = frozenset(
    {"MergeIdentityError", "MergeNameConflictError", "merge_manifests"}
)

_MERGE_COMMIT_EXPORTS = frozenset({"build_merge_commit", "find_commit_by_tree"})

_INGESTION_APPLY_EXPORTS = frozenset(
    {
        "apply_add_resource_transforms",
        "apply_add_resources",
        "apply_ensure_extracted_fields",
        "apply_remove_resources",
        "apply_set_inverse_emission",
    }
)

_ALIGNMENT_EXPORTS = frozenset(
    {
        "AlignmentAttribute",
        "AlignmentConflictError",
        "AlignmentRow",
        "DerivationSpec",
        "IdentityAlignment",
        "LocalKeySource",
        "LocalKeySpec",
        "SharedDerivation",
        "alignment_to_ops",
        "validate_alignment",
    }
)

_CANONICAL_EXPORTS = frozenset(
    {
        "CanonicalMap",
        "ClusterResolution",
        "ClusterSpec",
        "Completion",
        "MergeCanonicalConflictError",
        "MergeIncompleteError",
        "DanglingEntry",
        "DeclaredMaps",
        "SideMaps",
        "canonical_map_to_ops",
        "canonical_near_collisions",
        "canonicalize_ops",
        "check_member_existence",
        "clusters_to_side_maps",
        "dangling_entries",
        "fold_declared_maps",
        "compose_canonical_maps",
        "resolve_clusters",
        "same_name_groups",
        "trim_canonical_map",
        "validate_and_complete_canonical_map",
    }
)

_EQUIVALENCE_EXPORTS = frozenset(
    {
        "Cluster",
        "ClusterConflictError",
        "ClusterIndex",
        "ClusterResolution",
        "ClusterSpec",
        "RelationCluster",
        "UnknownMemberError",
        "check_member_existence",
        "index_clusters",
        "subject",
    }
)

_IDENTITY_EXPORTS = frozenset(
    {
        "apply_add_secondary_identities",
        "apply_remove_secondary_identities",
        "apply_replace_edge_identities",
        "apply_replace_identity",
    }
)

_STRUCTURE_EXPORTS = frozenset(
    {"apply_add_edges", "apply_add_vertices", "apply_retarget_edges"}
)

_PHYSICAL_EXPORTS = frozenset(
    {
        "apply_add_edge_indexes",
        "apply_add_vertex_indexes",
        "apply_change_field_types",
        "apply_remove_edge_indexes",
        "apply_remove_vertex_indexes",
        "apply_set_edge_directed",
        "apply_set_native_inverses",
    }
)

_SEMANTICS_EXPORTS = frozenset(
    {
        "apply_set_edge_semantics",
        "apply_set_field_semantics",
        "apply_set_vertex_descriptions",
        "apply_set_vertex_semantics",
    }
)

_CODEC_EXPORTS = frozenset(
    {
        "RetractEdgeInversesOp",
        "RevisionOp",
        "op_from_dict",
        "op_to_dict",
        "ops_from_dicts",
        "ops_from_yaml",
        "ops_to_dicts",
        "ops_to_yaml_str",
    }
)

_HASHING_EXPORTS = frozenset(
    {
        "full_hash",
        "graph_hash",
        "ingestion_hash",
        "manifest_hash",
        "schema_hash",
        "stable_hash",
    }
)

_CANONICALIZE_EXPORTS = frozenset(
    {
        "CANON_VERSION",
        "LIST_ORDER",
        "ListOrder",
        "UnclassifiedListField",
        "canonical_payload",
    }
)

_AUTOGENERATE_EXPORTS = frozenset(
    {"RenameHints", "diff_manifests", "diff_manifests_verified"}
)

_INVERSE_EXPORTS = frozenset(
    {"IRREVERSIBLE", "invert_op", "invert_ops", "irreversible_reason", "is_reversible"}
)

_INVERSE_PLAN_EXPORTS = frozenset(
    {
        "InversePlan",
        "plan_declare_symmetric",
        "plan_realize_inverses",
        "plan_repair_inverses",
        "plan_switch_realization",
        "plan_withdraw_realization",
    }
)

_COMMIT_EXPORTS = frozenset(
    {
        "COMMIT_KINDS",
        "Commit",
        "CommitError",
        "MergeRecipeRef",
        "build_commit",
        "build_root_commit",
        "compute_root_commit_id",
        "build_multi_parent_commit",
        "build_revert_commit",
        "compute_commit_id",
    }
)

_HISTORY_EXPORTS = frozenset(
    {
        "FileCommitStore",
        "History",
        "checkout",
        "verify_history",
    }
)

_MERGE3_EXPORTS = frozenset(
    {
        "ConflictResolution",
        "MergeConflict",
        "MergeError",
        "MergeRecipe",
        "MergeResult",
        "build_merge_recipe",
        "build_recipe",
        "describe_slot",
        "find_merge_base",
        "merge_three_way",
        "op_slots",
        "re_merge",
        "resolve_clusters",
        "take_left",
        "take_right",
    }
)

__all__ = [
    "CANON_VERSION",
    "COMMIT_KINDS",
    "INGESTION_REWRITING_OPS",
    "IRREVERSIBLE",
    "LIST_ORDER",
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
    "AlignmentAttribute",
    "AlignmentConflictError",
    "AlignmentRow",
    "AssignedIdentityTarget",
    "BlankIdentityTarget",
    "CanonicalMap",
    "CanonicalizeOp",
    "ChangeFieldTypesOp",
    "Cluster",
    "ClusterConflictError",
    "ClusterIndex",
    "Commit",
    "CommitError",
    "Completion",
    "ConflictResolution",
    "DanglingEntry",
    "DeclareEdgeInversesOp",
    "DeclaredMaps",
    "DerivationSpec",
    "EdgeFieldSemanticsTarget",
    "EdgeIdentitiesEntry",
    "EdgeIndexEntry",
    "EdgeRetargetEntry",
    "EdgeSelector",
    "EnsureExtractedFields",
    "EnsureExtractedFieldsOp",
    "FieldSemanticsTarget",
    "FieldTypeSpec",
    "FileCommitStore",
    # Revision layer
    "FunnelIdentityTarget",
    "HashIdentityTarget",
    "History",
    "IdentityAlignment",
    "IdentityReplacement",
    "IdentityTarget",
    "InversePlan",
    "ListOrder",
    "LocalKeySource",
    "LocalKeySpec",
    "ManifestOp",
    "MergeCanonicalConflictError",
    "MergeConflict",
    "MergeEdgesOp",
    "MergeError",
    "MergeIdentityError",
    "MergeIncompleteError",
    "MergeManifestsOp",
    "MergeNameConflictError",
    "MergeRecipe",
    "MergeRecipeRef",
    "MergeResult",
    "MergeVerticesOp",
    "NaturalIdentityTarget",
    "ProjectManifestOp",
    "PropertyEquivalence",
    "RelationCluster",
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
    "RenameHints",
    "RenameRelationsOp",
    "RenameResourcesOp",
    "RenameVertexPropertiesOp",
    "RenameVerticesOp",
    "ReplaceEdgeIdentitiesOp",
    "ReplaceIdentityOp",
    "RetargetEdgesOp",
    "RetractEdgeInversesOp",
    "RevisionOp",
    "SanitizeOp",
    "SetEdgeDirectedOp",
    "SetEdgeSemanticsOp",
    "SetFieldSemanticsOp",
    "SetInverseEmissionOp",
    "SetNativeInversesOp",
    "SetVertexDescriptionsOp",
    "SetVertexSemanticsOp",
    "SharedDerivation",
    "SideIdentity",
    "SideMaps",
    "UnclassifiedListField",
    "UnknownMemberError",
    "VertexEquivalence",
    "alignment_to_ops",
    "apply_add_edge_indexes",
    "apply_add_edge_properties",
    "apply_add_edges",
    "apply_add_inverse_edges",
    "apply_add_resource_transforms",
    "apply_add_resources",
    "apply_add_secondary_identities",
    "apply_add_vertex_indexes",
    "apply_add_vertex_properties",
    "apply_add_vertices",
    "apply_canonicalize",
    "apply_change_field_types",
    "apply_declare_edge_inverses",
    "apply_evolution",
    "apply_merge_edges",
    "apply_merge_vertices",
    "apply_project_manifest",
    "apply_remove_edge_ids",
    "apply_remove_edge_indexes",
    "apply_remove_edge_properties",
    "apply_remove_edges",
    "apply_remove_resources",
    "apply_remove_secondary_identities",
    "apply_remove_vertex_indexes",
    "apply_remove_vertex_properties",
    "apply_remove_vertices",
    "apply_rename_edge_properties",
    "apply_rename_relations",
    "apply_rename_resources",
    "apply_rename_vertex_properties",
    "apply_rename_vertices",
    "apply_replace_edge_identities",
    "apply_replace_identity",
    "apply_retarget_edges",
    "apply_retract_edge_inverses",
    "apply_sanitize",
    "apply_set_edge_directed",
    "apply_set_edge_semantics",
    "apply_set_field_semantics",
    "apply_set_inverse_emission",
    "apply_set_native_inverses",
    "apply_set_vertex_descriptions",
    "apply_set_vertex_semantics",
    "build_commit",
    "build_merge_commit",
    "build_merge_recipe",
    "build_multi_parent_commit",
    "build_recipe",
    "build_revert_commit",
    "build_root_commit",
    "canonical_map_to_ops",
    "canonical_near_collisions",
    "canonical_payload",
    "canonicalize_ops",
    "checkout",
    "clusters_to_side_maps",
    "compose_canonical_maps",
    "compute_commit_id",
    "compute_root_commit_id",
    "dangling_entries",
    "describe_slot",
    "diff_manifests",
    "diff_manifests_verified",
    "find_commit_by_tree",
    "find_merge_base",
    "fold_declared_maps",
    "full_hash",
    "graph_hash",
    "index_clusters",
    "ingestion_hash",
    "invert_op",
    "invert_ops",
    "irreversible_reason",
    "is_reversible",
    "manifest_hash",
    "merge_manifests",
    "merge_three_way",
    "op_from_dict",
    "op_slots",
    "op_to_dict",
    "ops_from_dicts",
    "ops_from_yaml",
    "ops_reaching_ingestion",
    "ops_to_dicts",
    "ops_to_yaml_str",
    "plan_declare_symmetric",
    "plan_realize_inverses",
    "plan_repair_inverses",
    "plan_switch_realization",
    "plan_withdraw_realization",
    "re_merge",
    "same_name_groups",
    "schema_hash",
    "stable_hash",
    "subject",
    "take_left",
    "take_right",
    "trim_canonical_map",
    "validate_alignment",
    "validate_and_complete_canonical_map",
    "verify_history",
]


def __getattr__(name: str) -> Any:
    if name in _APPLY_EXPORTS:
        from . import apply as apply_mod

        return getattr(apply_mod, name)
    if name in _MERGE_EXPORTS:
        from . import merge as merge_mod

        return getattr(merge_mod, name)
    if name in _MERGE_COMMIT_EXPORTS:
        from . import merge_commit as merge_commit_mod

        return getattr(merge_commit_mod, name)
    if name in _INGESTION_APPLY_EXPORTS:
        from . import ingestion as ingestion_mod

        return getattr(ingestion_mod, name)
    if name in _ALIGNMENT_EXPORTS:
        from . import alignment as alignment_mod

        return getattr(alignment_mod, name)
    if name in _CANONICAL_EXPORTS:
        from . import canonical as canonical_mod

        return getattr(canonical_mod, name)
    if name in _EQUIVALENCE_EXPORTS:
        from . import equivalence as equivalence_mod

        return getattr(equivalence_mod, name)
    if name in _IDENTITY_EXPORTS:
        from . import identity as identity_mod

        return getattr(identity_mod, name)
    if name in _STRUCTURE_EXPORTS:
        from . import structure as structure_mod

        return getattr(structure_mod, name)
    if name in _PHYSICAL_EXPORTS:
        from . import physical as physical_mod

        return getattr(physical_mod, name)
    if name in _SEMANTICS_EXPORTS:
        from . import semantics as semantics_mod

        return getattr(semantics_mod, name)
    if name in _CODEC_EXPORTS:
        from . import codec as codec_mod

        return getattr(codec_mod, name)
    if name in _HASHING_EXPORTS:
        from . import hashing as hashing_mod

        return getattr(hashing_mod, name)
    if name in _CANONICALIZE_EXPORTS:
        from . import canonicalize as canonicalize_mod

        return getattr(canonicalize_mod, name)
    if name in _AUTOGENERATE_EXPORTS:
        from . import autogenerate as autogenerate_mod

        return getattr(autogenerate_mod, name)
    if name in _INVERSE_EXPORTS:
        from . import inverse as inverse_mod

        return getattr(inverse_mod, name)
    if name in _INVERSE_PLAN_EXPORTS:
        from . import inverse_plan as inverse_plan_mod

        return getattr(inverse_plan_mod, name)
    if name in _COMMIT_EXPORTS:
        from . import commit as commit_mod

        return getattr(commit_mod, name)
    if name in _HISTORY_EXPORTS:
        from . import history as history_mod

        return getattr(history_mod, name)
    if name in _MERGE3_EXPORTS:
        from . import merge3 as merge3_mod

        return getattr(merge3_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
