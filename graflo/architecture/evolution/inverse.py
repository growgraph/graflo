"""Inverses for the subset of contract operations that have one.

Several ops are lossy — ``merge_vertices`` discards which source each property
came from, ``change_field_types`` discards the previous type when narrowing,
``sanitize`` and ``project_manifest`` drop material outright. There is no
information anywhere from which to reconstruct the prior state, so a generic
``downgrade()`` is not achievable and pretending otherwise would produce a
manifest that merely *looks* restored.

What is achievable is an inverse for the reversible subset, computed against the
**pre-state** manifest: inverting ``remove_vertices`` requires the removed
:class:`Vertex` models, and they exist only before the op runs.

:func:`invert_op` returns ``None`` for an irreversible op. Callers decide what
that means; :mod:`~graflo.architecture.evolution.revision` prefers replaying
from a base, which is always correct, and only falls back to inverses when no
base is available.
"""

from __future__ import annotations

import logging
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import Index

from .ops import (
    AddEdgeIndexesOp,
    AddEdgePropertiesOp,
    AddEdgesOp,
    AddInverseEdgesOp,
    AddSecondaryIdentitiesOp,
    AddVertexIndexesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    EdgeIndexEntry,
    EdgeRetargetEntry,
    EdgeSelector,
    ManifestOp,
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
    ReplaceIdentityOp,
    RetargetEdgesOp,
    SetEdgeDirectedOp,
)

logger = logging.getLogger(__name__)

#: Ops with no inverse, and why. Consulted by :func:`irreversible_reason`.
IRREVERSIBLE: dict[str, str] = {
    "merge_vertices": (
        "merging discards which source each property and identity came from"
    ),
    "merge_edges": "merging discards the source relations' individual definitions",
    "change_field_types": (
        "the previous field type is not recoverable once overwritten"
    ),
    "sanitize": "renames are flavor-driven and not recorded per element",
    "project_manifest": "projection drops elements outright",
    "compose_manifests": "composition is binary; there is no single prior manifest",
}


def is_reversible(op: ManifestOp) -> bool:
    """Whether *op* has a total inverse."""
    return getattr(op, "op", None) not in IRREVERSIBLE


def irreversible_reason(op: ManifestOp) -> str | None:
    """Why *op* cannot be inverted, or ``None`` when it can."""
    return IRREVERSIBLE.get(getattr(op, "op", ""))


def invert_op(op: ManifestOp, *, manifest: GraphManifest) -> ManifestOp | None:
    """The op undoing *op*, computed against the **pre-state** *manifest*.

    Returns ``None`` when *op* is irreversible. *manifest* must be the manifest
    as it was *before* *op* was applied — that is where the information an
    inverse needs still exists.
    """
    if not is_reversible(op):
        return None

    handler = _HANDLERS.get(op.op)
    if handler is None:
        logger.debug("no inverse handler for op %r", op.op)
        return None
    return handler(op, manifest)


def invert_ops(
    ops: list[ManifestOp], *, manifest: GraphManifest
) -> tuple[list[ManifestOp], list[str]]:
    """Inverses for *ops* in reverse order, plus reasons for any that lack one.

    Each inverse is computed against the state *before* its own op, so the ops
    are replayed forward to reconstruct those intermediate states.
    """
    from .apply import apply_evolution

    states: list[GraphManifest] = [manifest]
    current = manifest
    for op in ops:
        current = apply_evolution(current, [op], bump_version=False, finish_init=False)
        states.append(current)

    inverses: list[ManifestOp] = []
    blockers: list[str] = []
    for index in range(len(ops) - 1, -1, -1):
        op = ops[index]
        reason = irreversible_reason(op)
        if reason is not None:
            blockers.append(f"{op.op}: {reason}")
            continue
        inverse = invert_op(op, manifest=states[index])
        if inverse is None:
            blockers.append(f"{op.op}: no inverse could be derived")
            continue
        inverses.append(inverse)
    return inverses, blockers


# -- per-op inverses ----------------------------------------------------


def _invert_add_vertices(op: AddVerticesOp, _manifest: GraphManifest) -> ManifestOp:
    return RemoveVerticesOp(names=[vertex.name for vertex in op.vertices])


def _invert_remove_vertices(
    op: RemoveVerticesOp, manifest: GraphManifest
) -> ManifestOp | None:
    vertices = _vertices(manifest)
    restored = [vertices[name] for name in op.names if name in vertices]
    if len(restored) != len(op.names):
        return None
    return AddVerticesOp(vertices=restored)


def _invert_add_edges(op: AddEdgesOp, _manifest: GraphManifest) -> ManifestOp | None:
    relations = [edge.relation for edge in op.edges if edge.relation is not None]
    if len(relations) != len(op.edges):
        # RemoveEdgesOp addresses edges by relation name only.
        return None
    return RemoveEdgesOp(relations=relations)


def _invert_remove_edges(
    op: RemoveEdgesOp, manifest: GraphManifest
) -> ManifestOp | None:
    edges = [edge for edge in _edges(manifest) if edge.relation in set(op.relations)]
    if not edges:
        return None
    return AddEdgesOp(edges=edges)


def _invert_add_vertex_properties(
    op: AddVertexPropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RemoveVertexPropertiesOp(
        removals={name: list(fields) for name, fields in op.additions.items()}
    )


def _invert_remove_vertex_properties(
    op: RemoveVertexPropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return AddVertexPropertiesOp(
        additions={name: list(fields) for name, fields in op.removals.items()}
    )


def _invert_add_edge_properties(
    op: AddEdgePropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RemoveEdgePropertiesOp(
        removals={name: list(fields) for name, fields in op.additions.items()}
    )


def _invert_remove_edge_properties(
    op: RemoveEdgePropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return AddEdgePropertiesOp(
        additions={name: list(fields) for name, fields in op.removals.items()}
    )


def _invert_rename_vertices(
    op: RenameVerticesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameVerticesOp(vertices=_flip(op.vertices))


def _invert_rename_relations(
    op: RenameRelationsOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameRelationsOp(relations=_flip(op.relations))


def _invert_rename_resources(
    op: RenameResourcesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameResourcesOp(resources=_flip(op.resources))


def _invert_rename_vertex_properties(
    op: RenameVertexPropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameVertexPropertiesOp(
        renames={name: _flip(mapping) for name, mapping in op.renames.items()}
    )


def _invert_rename_edge_properties(
    op: RenameEdgePropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameEdgePropertiesOp(
        renames={name: _flip(mapping) for name, mapping in op.renames.items()}
    )


def _invert_add_vertex_indexes(
    op: AddVertexIndexesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RemoveVertexIndexesOp(
        indexes={
            name: [list(index.fields) for index in indexes]
            for name, indexes in op.indexes.items()
        }
    )


def _invert_remove_vertex_indexes(
    op: RemoveVertexIndexesOp, manifest: GraphManifest
) -> ManifestOp | None:
    profile = _profile(manifest)
    if profile is None:
        return None
    restored: dict[str, list[Index]] = {}
    for name, field_lists in op.indexes.items():
        wanted = {tuple(fields) for fields in field_lists}
        present = [
            index
            for index in profile.vertex_indexes.get(name, [])
            if tuple(index.fields) in wanted
        ]
        if len(present) != len(wanted):
            return None
        restored[name] = present
    return AddVertexIndexesOp(indexes=restored) if restored else None


def _invert_add_edge_indexes(
    op: AddEdgeIndexesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RemoveEdgeIndexesOp(
        edges=[
            EdgeIndexEntry(
                source=entry.source,
                target=entry.target,
                relation=entry.relation,
                purpose=entry.purpose,
                fields=[list(index.fields) for index in entry.indexes],
            )
            for entry in op.edges
        ]
    )


def _invert_remove_edge_indexes(
    op: RemoveEdgeIndexesOp, manifest: GraphManifest
) -> ManifestOp | None:
    profile = _profile(manifest)
    if profile is None:
        return None
    entries: list[EdgeIndexEntry] = []
    for entry in op.edges:
        spec = _edge_spec(profile, entry)
        if spec is None:
            return None
        wanted = {tuple(fields) for fields in entry.fields}
        present = [
            index
            for index in getattr(spec, "indexes", [])
            if tuple(index.fields) in wanted
        ]
        if len(present) != len(wanted):
            return None
        entries.append(
            EdgeIndexEntry(
                source=entry.source,
                target=entry.target,
                relation=entry.relation,
                purpose=entry.purpose,
                indexes=present,
            )
        )
    return AddEdgeIndexesOp(edges=entries) if entries else None


def _invert_set_edge_directed(
    op: SetEdgeDirectedOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Flip back only the edges that actually change, grouped by prior value."""
    by_prior: dict[bool, list[EdgeSelector]] = {True: [], False: []}
    existing = {(e.source, e.target, e.relation): e for e in _edges(manifest)}
    for selector in op.edges:
        edge = existing.get((selector.source, selector.target, selector.relation))
        if edge is None:
            return None
        by_prior[bool(edge.directed)].append(selector)

    groups = [(value, sel) for value, sel in by_prior.items() if sel]
    if len(groups) != 1:
        # Restoring mixed prior values needs two ops; the revision layer emits
        # one inverse per op, so refuse rather than restore half of it.
        return None
    directed, selectors = groups[0]
    return SetEdgeDirectedOp(edges=selectors, directed=directed)


def _invert_add_secondary_identities(
    op: AddSecondaryIdentitiesOp, _manifest: GraphManifest
) -> ManifestOp | None:
    removals: dict[str, list[str]] = {}
    for name, entries in op.additions.items():
        names = [entry.name for entry in entries if entry.name]
        if len(names) != len(entries):
            return None  # unnamed entries cannot be addressed for removal
        removals[name] = names
    return RemoveSecondaryIdentitiesOp(removals=removals)


def _invert_remove_secondary_identities(
    op: RemoveSecondaryIdentitiesOp, manifest: GraphManifest
) -> ManifestOp | None:
    vertices = _vertices(manifest)
    additions: dict[str, list[Any]] = {}
    for name, selectors in op.removals.items():
        vertex = vertices.get(name)
        if vertex is None:
            return None
        wanted = set(selectors)
        present = [s for s in vertex.secondary_identities if s.name in wanted]
        if len(present) != len(wanted):
            return None
        additions[name] = present
    return AddSecondaryIdentitiesOp(additions=additions) if additions else None


def _invert_retarget_edges(op: RetargetEdgesOp, _manifest: GraphManifest) -> ManifestOp:
    """Retarget back: the new endpoints select, the old ones become the target."""
    return RetargetEdgesOp(
        edges=[
            EdgeRetargetEntry(
                source=entry.new_source or entry.source,
                target=entry.new_target or entry.target,
                relation=entry.relation,
                new_source=entry.source if entry.new_source else None,
                new_target=entry.target if entry.new_target else None,
            )
            for entry in op.edges
        ]
    )


def _invert_add_inverse_edges(
    op: AddInverseEdgesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RemoveEdgesOp(relations=sorted(op.relations.values()))


def _invert_replace_identity(
    op: ReplaceIdentityOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore each vertex's prior identity policy, read from the pre-state."""
    from .autogenerate import _identity_target

    vertices = _vertices(manifest)
    restored: dict[str, dict[str, Any]] = {}
    for name, spec in op.vertices.items():
        vertex = vertices.get(name)
        if vertex is None:
            return None
        if spec.retire != "keep":
            # Demotion and dropping mutate secondary identities and properties
            # too; restoring only the primary key would leave the rest changed.
            return None
        restored[name] = {"to": _identity_target(vertex), "retire": "keep"}
    return ReplaceIdentityOp(vertices=restored) if restored else None


_HANDLERS: dict[str, Any] = {
    "add_vertices": _invert_add_vertices,
    "remove_vertices": _invert_remove_vertices,
    "add_edges": _invert_add_edges,
    "remove_edges": _invert_remove_edges,
    "add_vertex_properties": _invert_add_vertex_properties,
    "remove_vertex_properties": _invert_remove_vertex_properties,
    "add_edge_properties": _invert_add_edge_properties,
    "remove_edge_properties": _invert_remove_edge_properties,
    "rename_vertices": _invert_rename_vertices,
    "rename_relations": _invert_rename_relations,
    "rename_resources": _invert_rename_resources,
    "rename_vertex_properties": _invert_rename_vertex_properties,
    "rename_edge_properties": _invert_rename_edge_properties,
    "add_vertex_indexes": _invert_add_vertex_indexes,
    "remove_vertex_indexes": _invert_remove_vertex_indexes,
    "add_edge_indexes": _invert_add_edge_indexes,
    "remove_edge_indexes": _invert_remove_edge_indexes,
    "set_edge_directed": _invert_set_edge_directed,
    "add_secondary_identities": _invert_add_secondary_identities,
    "remove_secondary_identities": _invert_remove_secondary_identities,
    "retarget_edges": _invert_retarget_edges,
    "add_inverse_edges": _invert_add_inverse_edges,
    "replace_identity": _invert_replace_identity,
}


# -- accessors ----------------------------------------------------------


def _flip(mapping: dict[str, str]) -> dict[str, str]:
    """Invert a rename map.

    Sound only because the rename ops reject non-injective maps at construction
    (``validate_rename_map_is_injective``): flipping ``{a: c, b: c}`` would silently
    drop one entry and make the "inverse" lossy. The flipped map is itself validated
    when the inverse op is constructed, so a violation surfaces rather than replaying
    a corrupted manifest.
    """
    return {new: old for old, new in mapping.items()}


def _vertices(manifest: GraphManifest) -> dict[str, Any]:
    schema = manifest.graph_schema
    if schema is None:
        return {}
    return {v.name: v for v in schema.core_schema.vertex_config.vertices}


def _edges(manifest: GraphManifest) -> list[Any]:
    schema = manifest.graph_schema
    if schema is None:
        return []
    return list(schema.core_schema.edge_config.edges)


def _profile(manifest: GraphManifest) -> Any:
    schema = manifest.graph_schema
    return None if schema is None else schema.db_profile


def _edge_spec(profile: Any, entry: EdgeIndexEntry) -> Any:
    key = (entry.source, entry.target, entry.relation, entry.purpose)
    for spec in getattr(profile, "edge_specs", []):
        if spec.physical_key == key:
            return spec
    return None


__all__ = [
    "IRREVERSIBLE",
    "invert_op",
    "invert_ops",
    "irreversible_reason",
    "is_reversible",
]
