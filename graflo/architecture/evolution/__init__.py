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

_COMPOSE_EXPORTS = frozenset({"compose_manifests"})

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

_AUTOGENERATE_EXPORTS = frozenset(
    {"RenameHints", "diff_manifests", "diff_manifests_verified"}
)

_INVERSE_EXPORTS = frozenset(
    {"IRREVERSIBLE", "invert_op", "invert_ops", "irreversible_reason", "is_reversible"}
)

_REVISION_EXPORTS = frozenset(
    {
        "FileRevisionStore",
        "Revision",
        "RevisionChain",
        "RevisionError",
        "apply_revisions",
        "build_revision",
        "compute_revision_id",
        "downgrade_to",
    }
)

__all__ = [
    "INGESTION_REWRITING_OPS",
    "IRREVERSIBLE",
    "AddEdgeIndexesOp",
    "AddEdgePropertiesOp",
    "AddEdgesOp",
    "AddInverseEdgesOp",
    "AddSecondaryIdentitiesOp",
    "AddVertexIndexesOp",
    "AddVertexPropertiesOp",
    "AddVerticesOp",
    "AssignedIdentityTarget",
    "BlankIdentityTarget",
    "ChangeFieldTypesOp",
    "ComposeManifestsOp",
    "EdgeIdentitiesEntry",
    "EdgeIndexEntry",
    "EdgeRetargetEntry",
    "EdgeSelector",
    "FieldTypeSpec",
    # Revision layer
    "FileRevisionStore",
    "HashIdentityTarget",
    "IdentityReplacement",
    "IdentityTarget",
    "ManifestOp",
    "MergeEdgesOp",
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
    "RetargetEdgesOp",
    "Revision",
    "RevisionChain",
    "RevisionError",
    "RevisionOp",
    "SanitizeOp",
    "SetEdgeDirectedOp",
    "VertexEquivalence",
    "apply_add_edge_indexes",
    "apply_add_edge_properties",
    "apply_add_edges",
    "apply_add_inverse_edges",
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
    "apply_revisions",
    "apply_sanitize",
    "apply_set_edge_directed",
    "build_revision",
    "compose_manifests",
    "compute_revision_id",
    "diff_manifests",
    "diff_manifests_verified",
    "downgrade_to",
    "full_hash",
    "graph_hash",
    "ingestion_hash",
    "invert_op",
    "invert_ops",
    "irreversible_reason",
    "is_reversible",
    "manifest_hash",
    "op_from_dict",
    "op_to_dict",
    "ops_from_dicts",
    "ops_from_yaml",
    "ops_reaching_ingestion",
    "ops_to_dicts",
    "ops_to_yaml_str",
    "schema_hash",
    "stable_hash",
]


def __getattr__(name: str) -> Any:
    if name in _APPLY_EXPORTS:
        from . import apply as apply_mod

        return getattr(apply_mod, name)
    if name in _COMPOSE_EXPORTS:
        from . import compose as compose_mod

        return getattr(compose_mod, name)
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
    if name in _AUTOGENERATE_EXPORTS:
        from . import autogenerate as autogenerate_mod

        return getattr(autogenerate_mod, name)
    if name in _INVERSE_EXPORTS:
        from . import inverse as inverse_mod

        return getattr(inverse_mod, name)
    if name in _REVISION_EXPORTS:
        from . import revision as revision_mod

        return getattr(revision_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
