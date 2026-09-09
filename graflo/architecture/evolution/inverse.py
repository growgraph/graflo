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
    AddResourcesOp,
    AddSecondaryIdentitiesOp,
    AddVertexIndexesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    EdgeFieldSemanticsTarget,
    EdgeIdentitiesEntry,
    EdgeIndexEntry,
    EdgeRetargetEntry,
    EdgeSelector,
    FieldSemanticsTarget,
    ManifestOp,
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
    SetEdgeDirectedOp,
    SetEdgeSemanticsOp,
    SetFieldSemanticsOp,
    SetVertexSemanticsOp,
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
    "add_resource_transforms": (
        "appended pipeline steps are not tracked per-op; there is no "
        "remove_resource_transforms op"
    ),
    "ensure_extracted_fields": (
        "widened projections are not tracked per-op; there is no narrowing op"
    ),
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
    from .hashing import manifest_hash

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
            if manifest_hash(states[index]) == manifest_hash(states[index + 1]):
                # The forward op changed nothing (every entry it named was
                # already there), so the identity is its inverse.
                continue
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


def _selectors(edges: list[Any]) -> list[EdgeSelector]:
    return [
        EdgeSelector(source=edge.source, target=edge.target, relation=edge.relation)
        for edge in edges
    ]


def _invert_add_edges(op: AddEdgesOp, _manifest: GraphManifest) -> ManifestOp:
    # ``apply_add_edges`` rejects an edge that already exists, so every listed
    # triple is new and removing exactly those triples is the exact inverse.
    return RemoveEdgesOp(edges=_selectors(op.edges))


def _invert_remove_edges(
    op: RemoveEdgesOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore every edge the op addressed, by relation and by triple alike."""
    relations = set(op.relations)
    edge_ids = {selector.edge_id() for selector in op.edges}
    edges = [
        edge
        for edge in _edges(manifest)
        if edge.relation in relations or edge.edge_id in edge_ids
    ]
    if not edges or edge_ids - {edge.edge_id for edge in edges}:
        return None
    return AddEdgesOp(edges=edges)


def _invert_add_vertex_properties(
    op: AddVertexPropertiesOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Remove only what the op added: a name already present is skipped forward."""
    vertices = _vertices(manifest)
    removals: dict[str, list[str]] = {}
    for name in op.additions:
        vertex = vertices.get(name)
        if vertex is None:
            return None
        present = {field.name for field in vertex.properties}
        added = [field for field in op.field_names(name) if field not in present]
        if added:
            removals[name] = added
    return RemoveVertexPropertiesOp(removals=removals) if removals else None


def _invert_remove_vertex_properties(
    op: RemoveVertexPropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return AddVertexPropertiesOp(
        additions={name: list(fields) for name, fields in op.removals.items()}
    )


def _invert_add_edge_properties(
    op: AddEdgePropertiesOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Remove only what the op added, across every edge carrying the relation.

    The forward op is per edge (a name already on one edge is skipped there)
    while both ops are addressed by relation, so a field present on some sibling
    edges and absent on others has no relation-wide inverse; ``None`` says so
    rather than removing it from the edges that had it before.
    """
    by_relation: dict[str, list[Any]] = {}
    for edge in _edges(manifest):
        if edge.relation is not None:
            by_relation.setdefault(edge.relation, []).append(edge)
    removals: dict[str, list[str]] = {}
    for relation in op.additions:
        edges = by_relation.get(relation)
        if not edges:
            return None
        added: list[str] = []
        for field in op.field_names(relation):
            presence = {any(f.name == field for f in edge.properties) for edge in edges}
            if presence == {False}:
                added.append(field)
            elif presence != {True}:
                return None
        if added:
            removals[relation] = added
    return RemoveEdgePropertiesOp(removals=removals) if removals else None


def _invert_remove_edge_properties(
    op: RemoveEdgePropertiesOp, _manifest: GraphManifest
) -> ManifestOp:
    return AddEdgePropertiesOp(
        additions={name: list(fields) for name, fields in op.removals.items()}
    )


def _invert_rename_vertices(
    op: RenameVerticesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameVerticesOp(renames=_flip(op.renames))


def _invert_rename_relations(
    op: RenameRelationsOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameRelationsOp(renames=_flip(op.renames))


def _invert_rename_resources(
    op: RenameResourcesOp, _manifest: GraphManifest
) -> ManifestOp:
    return RenameResourcesOp(renames=_flip(op.renames))


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
    op: AddVertexIndexesOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Remove only the field-sets the op registered; registration is idempotent."""
    profile = _profile(manifest)
    removals: dict[str, list[list[str]]] = {}
    for name, indexes in op.indexes.items():
        present = (
            {tuple(ix.fields) for ix in profile.vertex_indexes.get(name, [])}
            if profile is not None
            else set()
        )
        added = [list(ix.fields) for ix in indexes if tuple(ix.fields) not in present]
        if added:
            removals[name] = added
    return RemoveVertexIndexesOp(indexes=removals) if removals else None


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
    """Restore the withdrawn entries, resolving selectors the way the forward op does.

    A selector is a name, a field list, or the bare ``secondary`` shorthand;
    only the forward op's matcher knows all three, so it is reused rather than
    re-implemented against names alone.
    """
    from .identity import _matches_selector

    vertices = _vertices(manifest)
    additions: dict[str, list[Any]] = {}
    for name, selectors in op.removals.items():
        vertex = vertices.get(name)
        if vertex is None:
            return None
        declared = len(vertex.secondary_identities)
        present: list[Any] = []
        for selector in selectors:
            matched = [
                entry
                for entry in vertex.secondary_identities
                if _matches_selector(entry, selector, declared)
            ]
            if len(matched) != 1:
                return None
            if matched[0] not in present:
                present.append(matched[0])
        additions[name] = present
    return AddSecondaryIdentitiesOp(additions=additions) if additions else None


def _invert_replace_edge_identities(
    op: ReplaceEdgeIdentitiesOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore each edge's prior uniqueness keys from the pre-state.

    Partial by design: ``Edge.finish_init`` folds every non-endpoint identity
    token into ``properties``, and no single op both restores the keys and
    removes those properties. A new key naming a token that is neither an
    endpoint nor an existing property therefore has no inverse; returning
    ``None`` says so rather than restoring the keys and leaving the property.
    """
    existing = {(e.source, e.target, e.relation): e for e in _edges(manifest)}
    entries: list[EdgeIdentitiesEntry] = []
    for entry in op.edges:
        edge = existing.get(entry.edge_id())
        if edge is None:
            return None
        declared = {field.name for field in edge.properties} | {"source", "target"}
        if any(token not in declared for key in entry.identities for token in key):
            return None
        entries.append(
            EdgeIdentitiesEntry(
                source=entry.source,
                target=entry.target,
                relation=entry.relation,
                identities=[list(key) for key in edge.identities],
            )
        )
    return ReplaceEdgeIdentitiesOp(edges=entries)


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
    op: AddInverseEdgesOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Remove exactly the edges the forward op would create against this pre-state.

    Derived with the forward op's own edge derivation, so an inverse relation
    that already existed elsewhere, or a reverse edge synthesized for a
    relation-less directed edge, is accounted for the same way in both
    directions.
    """
    from .inverse_edges import _schema_edges_with_inverses

    before = _edges(manifest)
    existing = {edge.edge_id for edge in before}
    after = _schema_edges_with_inverses(
        list(before), dict(op.inverses), _profile(manifest)
    )
    created = [edge for edge in after if edge.edge_id not in existing]
    if not created:
        return None
    return RemoveEdgesOp(edges=_selectors(created))


def _invert_replace_identity(
    op: ReplaceIdentityOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore each vertex's prior identity policy, read from the pre-state."""
    from .autogenerate import _identity_target

    vertices = _vertices(manifest)
    restored: dict[str, dict[str, Any]] = {}
    for name, spec in op.replacements.items():
        vertex = vertices.get(name)
        if vertex is None:
            return None
        if spec.retire != "keep":
            # Demotion and dropping mutate secondary identities and properties
            # too; restoring only the primary key would leave the rest changed.
            return None
        restored[name] = {"to": _identity_target(vertex), "retire": "keep"}
    return ReplaceIdentityOp(replacements=restored) if restored else None


def _invert_set_vertex_semantics(
    op: SetVertexSemanticsOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore each vertex's prior grounding, ``None`` included.

    Clearing is expressible (the payload value is optional), so grounding is
    cleanly reversible: the inverse of "ground it" is "put back whatever was
    there", which for a previously ungrounded type is nothing.
    """
    schema = manifest.graph_schema
    if schema is None:
        return None
    by_name = {v.name: v for v in schema.core_schema.vertex_config.vertices}
    prior: dict[str, Any] = {}
    for name in op.semantics:
        vertex = by_name.get(name)
        if vertex is None:
            return None
        prior[name] = vertex.semantics
    return SetVertexSemanticsOp(semantics=prior)


def _invert_set_edge_semantics(
    op: SetEdgeSemanticsOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore the prior grounding, but only when every selected edge shared it.

    The payload carries one value for the whole selection, so a selection whose
    edges were grounded differently has no single inverse expressible as one op.
    Returning ``None`` says "not invertible" rather than inventing a value that
    would silently flatten them.
    """
    existing = {(e.source, e.target, e.relation): e for e in _edges(manifest)}
    priors: list[Any] = []
    for selector in op.edges:
        edge = existing.get((selector.source, selector.target, selector.relation))
        if edge is None:
            return None
        priors.append(edge.semantics)

    first = priors[0]
    if any(prior != first for prior in priors[1:]):
        return None
    return SetEdgeSemanticsOp(edges=list(op.edges), semantics=first)


def _invert_set_field_semantics(
    op: SetFieldSemanticsOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore each property's prior grounding. Per-target, so always expressible."""
    schema = manifest.graph_schema
    if schema is None:
        return None
    by_name = {v.name: v for v in schema.core_schema.vertex_config.vertices}
    by_edge_id = {e.edge_id: e for e in _edges(manifest)}
    targets: list[FieldSemanticsTarget | EdgeFieldSemanticsTarget] = []
    for target in op.targets:
        holder = (
            by_name.get(target.vertex)
            if isinstance(target, FieldSemanticsTarget)
            else by_edge_id.get(target.edge_id())
        )
        if holder is None:
            return None
        field = next((f for f in holder.properties if f.name == target.field), None)
        if field is None:
            return None
        targets.append(target.model_copy(update={"semantics": field.semantics}))
    return SetFieldSemanticsOp(targets=targets)


def _invert_add_resources(op: AddResourcesOp, _manifest: GraphManifest) -> ManifestOp:
    # ``apply_add_resources`` rejects a name already present, so every listed
    # resource is new.
    return RemoveResourcesOp(names=[resource.name for resource in op.resources])


def _invert_remove_resources(
    op: RemoveResourcesOp, manifest: GraphManifest
) -> ManifestOp | None:
    """Restore the removed resources -- unless bindings wired them.

    Removal also prunes the ``resource_connector`` entries naming the resource,
    and no op restores those, so a wired resource has no single-op inverse.
    """
    if manifest.ingestion_model is None:
        return None
    by_name = {r.name: r for r in manifest.ingestion_model.resources}
    if any(name not in by_name for name in op.names):
        return None
    removed = set(op.names)
    if manifest.bindings is not None and any(
        _bound_resource(entry) in removed
        for entry in manifest.bindings.resource_connector
    ):
        return None
    return AddResourcesOp(resources=[by_name[name] for name in op.names])


def _bound_resource(entry: Any) -> str | None:
    """The resource a ``resource_connector`` entry names, whether model or mapping."""
    if isinstance(entry, dict):
        value = entry.get("resource")
        return value if isinstance(value, str) else None
    return entry.resource


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
    "set_vertex_semantics": _invert_set_vertex_semantics,
    "set_edge_semantics": _invert_set_edge_semantics,
    "set_field_semantics": _invert_set_field_semantics,
    "add_secondary_identities": _invert_add_secondary_identities,
    "remove_secondary_identities": _invert_remove_secondary_identities,
    "retarget_edges": _invert_retarget_edges,
    "add_inverse_edges": _invert_add_inverse_edges,
    "replace_identity": _invert_replace_identity,
    "replace_edge_identities": _invert_replace_edge_identities,
    "add_resources": _invert_add_resources,
    "remove_resources": _invert_remove_resources,
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
