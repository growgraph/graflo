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
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexIndexesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    AssignedIdentityTarget,
    BlankIdentityTarget,
    ChangeFieldTypesOp,
    ComposeManifestsOp,
    EdgeIdentitiesEntry,
    EdgeIndexEntry,
    EdgeRetargetEntry,
    EdgeSelector,
    FieldTypeSpec,
    FunnelIdentityTarget,
    HashIdentityTarget,
    IdentityReplacement,
    IdentityTarget,
    ManifestOp,
    MergeEdgesOp,
    MergeVerticesOp,
    NaturalIdentityTarget,
    ProjectManifestOp,
    PropertyEquivalence,
    RelationEquivalence,
    RemoveEdgeIndexesOp,
    RemoveEdgePropertiesOp,
    RemoveEdgesOp,
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
    SanitizeOp,
    SetEdgeDirectedOp,
    VertexEquivalence,
    ops_reaching_ingestion,
)

_APPLY_EXPORTS = frozenset(
    {
        "apply_evolution",
        "apply_add_edge_properties",
        "apply_add_inverse_edges",
        "apply_add_vertex_properties",
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
        "apply_sanitize",
    }
)

_COMPOSE_EXPORTS = frozenset({"ComposeNameConflictError", "compose_manifests"})

_INGESTION_APPLY_EXPORTS = frozenset({"apply_add_resource_transforms"})

_ALIGNMENT_EXPORTS = frozenset(
    {
        "AlignmentConflictError",
        "AlignmentRow",
        "DerivationSpec",
        "IdentityAlignment",
        "LocalKeySource",
        "LocalKeySpec",
        "alignment_to_ops",
        "validate_alignment",
    }
)

_CANONICAL_EXPORTS = frozenset(
    {
        "CanonicalMap",
        "ComposeCanonicalConflictError",
        "canonical_map_to_ops",
        "validate_and_complete_canonical_map",
        "validate_compose_against_canonical_map",
    }
)

_EQUIVALENCE_EXPORTS = frozenset(
    {
        "Cluster",
        "ClusterConflictError",
        "ResolvedCluster",
        "build_clusters",
        "resolve_cluster_labels",
        "vertex_rename_maps",
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
    }
)

_CODEC_EXPORTS = frozenset(
    {
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

_COMMIT_EXPORTS = frozenset(
    {
        "COMMIT_KINDS",
        "Commit",
        "CommitError",
        "MergeRecipeRef",
        "build_commit",
        "build_merge_commit",
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
        "build_recipe",
        "describe_slot",
        "find_merge_base",
        "merge_three_way",
        "op_slots",
        "re_merge",
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
    "AddSecondaryIdentitiesOp",
    "AddVertexIndexesOp",
    "AddVertexPropertiesOp",
    "AddVerticesOp",
    "AlignmentConflictError",
    "AlignmentRow",
    "AssignedIdentityTarget",
    "BlankIdentityTarget",
    "CanonicalMap",
    "ChangeFieldTypesOp",
    "Cluster",
    "ClusterConflictError",
    "Commit",
    "CommitError",
    "ComposeCanonicalConflictError",
    "ComposeManifestsOp",
    "ComposeNameConflictError",
    "ConflictResolution",
    "DerivationSpec",
    "EdgeIdentitiesEntry",
    "EdgeIndexEntry",
    "EdgeRetargetEntry",
    "EdgeSelector",
    "FieldTypeSpec",
    "FileCommitStore",
    # Revision layer
    "FunnelIdentityTarget",
    "HashIdentityTarget",
    "History",
    "IdentityAlignment",
    "IdentityReplacement",
    "IdentityTarget",
    "ListOrder",
    "LocalKeySource",
    "LocalKeySpec",
    "ManifestOp",
    "MergeConflict",
    "MergeEdgesOp",
    "MergeError",
    "MergeRecipe",
    "MergeRecipeRef",
    "MergeResult",
    "MergeVerticesOp",
    "NaturalIdentityTarget",
    "ProjectManifestOp",
    "PropertyEquivalence",
    "RelationEquivalence",
    "RemoveEdgeIndexesOp",
    "RemoveEdgePropertiesOp",
    "RemoveEdgesOp",
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
    "ResolvedCluster",
    "RetargetEdgesOp",
    "RevisionOp",
    "SanitizeOp",
    "SetEdgeDirectedOp",
    "UnclassifiedListField",
    "VertexEquivalence",
    "alignment_to_ops",
    "apply_add_edge_indexes",
    "apply_add_edge_properties",
    "apply_add_edges",
    "apply_add_inverse_edges",
    "apply_add_resource_transforms",
    "apply_add_secondary_identities",
    "apply_add_vertex_indexes",
    "apply_add_vertex_properties",
    "apply_add_vertices",
    "apply_change_field_types",
    "apply_evolution",
    "apply_merge_edges",
    "apply_merge_vertices",
    "apply_project_manifest",
    "apply_remove_edge_ids",
    "apply_remove_edge_indexes",
    "apply_remove_edge_properties",
    "apply_remove_edges",
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
    "apply_sanitize",
    "apply_set_edge_directed",
    "build_clusters",
    "build_commit",
    "build_merge_commit",
    "build_recipe",
    "build_revert_commit",
    "canonical_map_to_ops",
    "canonical_payload",
    "checkout",
    "compose_manifests",
    "compute_commit_id",
    "describe_slot",
    "diff_manifests",
    "diff_manifests_verified",
    "find_merge_base",
    "full_hash",
    "graph_hash",
    "ingestion_hash",
    "invert_op",
    "invert_ops",
    "irreversible_reason",
    "is_reversible",
    "manifest_hash",
    "merge_three_way",
    "op_from_dict",
    "op_slots",
    "op_to_dict",
    "ops_from_dicts",
    "ops_from_yaml",
    "ops_reaching_ingestion",
    "ops_to_dicts",
    "ops_to_yaml_str",
    "re_merge",
    "resolve_cluster_labels",
    "schema_hash",
    "stable_hash",
    "take_left",
    "take_right",
    "validate_alignment",
    "validate_and_complete_canonical_map",
    "validate_compose_against_canonical_map",
    "verify_history",
    "vertex_rename_maps",
]


def __getattr__(name: str) -> Any:
    if name in _APPLY_EXPORTS:
        from . import apply as apply_mod

        return getattr(apply_mod, name)
    if name in _COMPOSE_EXPORTS:
        from . import compose as compose_mod

        return getattr(compose_mod, name)
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
