"""Derive a contract change set from two manifests.

Before this, nothing produced :data:`~graflo.architecture.evolution.ops.ManifestOp`
values: every op in the codebase was hand-built. ``SchemaDiff`` (``migrate``)
produces *description records* on a different plane — it reports what changed
but emits nothing that can be applied — and it only ever looks at ``Schema``,
never at ``ingestion_model`` or ``bindings``.

:func:`diff_manifests` closes that gap for the mechanically derivable ops, and
is explicit about the rest. Its contract is the **replay invariant**::

    ops, warnings = diff_manifests(base, target)
    manifest_hash(apply_evolution(base, ops)) == manifest_hash(target)

Where that does not hold, :func:`diff_manifests_verified` reports the residual
rather than claiming success. Silence about an incomplete diff is the one
failure mode a change-set generator must not have — it produces a revision that
looks applied and is not.

Renames are ambiguous by construction: a dropped ``mail`` plus an added
``email`` is indistinguishable from a rename. Pass :class:`RenameHints` when the
intent is known; otherwise the pair is emitted as a drop and an add.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import Index
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Vertex

from .ops import (
    AddEdgeIndexesOp,
    AddEdgePropertiesOp,
    AddEdgesOp,
    AddSecondaryIdentitiesOp,
    AddVertexIndexesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    ChangeFieldTypesOp,
    EdgeIndexEntry,
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
    SetEdgeDirectedOp,
    validate_rename_map_is_injective,
)

logger = logging.getLogger(__name__)


class RenameHints(ConfigBaseModel):
    """Renames the differ cannot infer, supplied by the caller.

    A drop plus an add is structurally identical to a rename. Guessing would
    turn a data-preserving rename into a destructive drop (or the reverse), so
    the differ never guesses.
    """

    vertices: dict[str, str] = PydanticField(
        default_factory=dict, description="``{old_vertex_name: new_vertex_name}``."
    )
    relations: dict[str, str] = PydanticField(
        default_factory=dict, description="``{old_relation: new_relation}``."
    )
    resources: dict[str, str] = PydanticField(
        default_factory=dict, description="``{old_resource: new_resource}``."
    )
    vertex_properties: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description="``{vertex_name: {old_field: new_field}}``.",
    )
    edge_properties: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description="``{relation: {old_field: new_field}}``.",
    )

    @model_validator(mode="after")
    def _reject_collapsing_maps(self) -> RenameHints:
        """Reject hints that would collapse two names onto one.

        The hints are handed to the rename ops verbatim, and the differ itself keys
        by the renamed name (``hints.vertices.get(name, name)``), so a collapsing
        hint corrupts the *diff* before any op is applied.
        """
        validate_rename_map_is_injective(
            self.vertices,
            kind="rename hint: vertices",
            merge_hint="MergeVerticesOp(sources=[...], into=...)",
        )
        validate_rename_map_is_injective(
            self.relations,
            kind="rename hint: relations",
            merge_hint="MergeEdgesOp(sources=[...], into=...)",
        )
        validate_rename_map_is_injective(
            self.resources,
            kind="rename hint: resources",
            merge_hint="ComposeManifestsOp with explicit resource_renames",
        )
        for vertex_name, field_renames in self.vertex_properties.items():
            validate_rename_map_is_injective(
                field_renames,
                kind=f"rename hint: vertex_properties[{vertex_name!r}]",
                merge_hint="RemoveVertexPropertiesOp to drop the redundant field first",
            )
        for relation, field_renames in self.edge_properties.items():
            validate_rename_map_is_injective(
                field_renames,
                kind=f"rename hint: edge_properties[{relation!r}]",
                merge_hint="RemoveEdgePropertiesOp to drop the redundant field first",
            )
        return self


def diff_manifests(
    base: GraphManifest,
    target: GraphManifest,
    *,
    hints: RenameHints | None = None,
) -> tuple[list[ManifestOp], list[str]]:
    """Ops turning *base* into *target*, plus warnings for what was not expressed.

    Ops are ordered so each one's preconditions hold when it runs: renames
    first (so later ops address the new names), then additions, then property
    and identity changes, then removals last.
    """
    hints = hints or RenameHints()
    warnings: list[str] = []
    ops: list[ManifestOp] = []

    ops += _rename_ops(hints)
    ops += _vertex_structure_ops(base, target, hints, warnings)
    ops += _edge_structure_ops(base, target, hints, warnings)
    ops += _vertex_property_ops(base, target, hints)
    ops += _edge_property_ops(base, target, hints)
    ops += _identity_ops(base, target, hints, warnings)
    ops += _index_ops(base, target, hints)
    ops += _removal_ops(base, target, hints)

    _warn_unexpressed(base, target, warnings, hints)
    return ops, warnings


def diff_manifests_verified(
    base: GraphManifest,
    target: GraphManifest,
    *,
    hints: RenameHints | None = None,
) -> tuple[list[ManifestOp], list[str]]:
    """:func:`diff_manifests`, with the replay invariant actually checked.

    Applies the derived ops to a copy of *base* and compares the result's hash
    to *target*'s. A mismatch appends a warning naming the residual difference
    instead of letting an incomplete change set pass as complete.
    """
    from .apply import apply_evolution
    from .hashing import manifest_hash

    ops, warnings = diff_manifests(base, target, hints=hints)
    if not ops:
        if manifest_hash(base) != manifest_hash(target):
            warnings.append(
                "manifests differ but no operation was derived; the change is "
                "not expressible in the current op vocabulary"
            )
        return ops, warnings

    try:
        # No version bump: the hash comparison is about content, and a bump
        # would make every verified diff report a spurious difference.
        replayed = apply_evolution(base, ops, bump_version=False, finish_init=False)
    except Exception as exc:
        warnings.append(f"derived operations do not apply cleanly: {exc}")
        return ops, warnings

    if manifest_hash(replayed) != manifest_hash(target):
        warnings.append(
            "replaying the derived operations does not reproduce the target "
            f"manifest; residual: {_residual(replayed, target)}"
        )
    return ops, warnings


# -- ordering stages ----------------------------------------------------


def _rename_ops(hints: RenameHints) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    if hints.vertices:
        ops.append(RenameVerticesOp(vertices=dict(hints.vertices)))
    if hints.relations:
        ops.append(RenameRelationsOp(relations=dict(hints.relations)))
    if hints.resources:
        ops.append(RenameResourcesOp(resources=dict(hints.resources)))
    if hints.vertex_properties:
        ops.append(
            RenameVertexPropertiesOp(
                renames={k: dict(v) for k, v in hints.vertex_properties.items()}
            )
        )
    if hints.edge_properties:
        ops.append(
            RenameEdgePropertiesOp(
                renames={k: dict(v) for k, v in hints.edge_properties.items()}
            )
        )
    return ops


def _vertex_structure_ops(
    base: GraphManifest,
    target: GraphManifest,
    hints: RenameHints,
    warnings: list[str],
) -> list[ManifestOp]:
    base_names = _renamed_vertex_names(base, hints)
    added = [v for name, v in _vertices(target).items() if name not in base_names]
    if not added:
        return []
    return [AddVerticesOp(vertices=added)]


def _edge_structure_ops(
    base: GraphManifest,
    target: GraphManifest,
    hints: RenameHints,
    warnings: list[str],
) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    base_edges = _edges_after_renames(base, hints)
    target_edges = _edges(target)

    added = [edge for key, edge in target_edges.items() if key not in base_edges]
    if added:
        ops.append(AddEdgesOp(edges=added))

    flipped: dict[bool, list[EdgeSelector]] = {True: [], False: []}
    for key, edge in target_edges.items():
        old = base_edges.get(key)
        if old is not None and bool(old.directed) != bool(edge.directed):
            flipped[bool(edge.directed)].append(
                EdgeSelector(source=key[0], target=key[1], relation=key[2])
            )
    for directed, selectors in flipped.items():
        if selectors:
            ops.append(SetEdgeDirectedOp(edges=selectors, directed=directed))
    return ops


def _vertex_property_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints
) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    base_vertices = _vertices_after_renames(base, hints)
    target_vertices = _vertices(target)

    additions: dict[str, list[str]] = {}
    removals: dict[str, list[str]] = {}
    type_changes: dict[str, dict[str, dict[str, Any]]] = {}

    for name, new_vertex in target_vertices.items():
        old_vertex = base_vertices.get(name)
        if old_vertex is None:
            continue
        old_fields = {f.name: f for f in old_vertex.properties}
        new_fields = {f.name: f for f in new_vertex.properties}

        gained = [f for f in new_fields if f not in old_fields]
        lost = [f for f in old_fields if f not in new_fields]
        if gained:
            additions[name] = gained
        if lost:
            removals[name] = lost

        changed = {
            field: _type_spec(new_fields[field])
            for field in new_fields
            if field in old_fields
            and _type_of(old_fields[field]) != _type_of(new_fields[field])
        }
        if changed:
            type_changes[name] = changed

    if additions:
        ops.append(AddVertexPropertiesOp(additions=additions))
    if type_changes:
        ops.append(ChangeFieldTypesOp(vertices=type_changes))
    if removals:
        ops.append(RemoveVertexPropertiesOp(removals=removals))
    return ops


def _edge_property_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints
) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    base_edges = _edges_after_renames(base, hints)
    target_edges = _edges(target)

    additions: dict[str, list[str]] = {}
    removals: dict[str, list[str]] = {}

    for key, new_edge in target_edges.items():
        old_edge = base_edges.get(key)
        relation = key[2]
        if old_edge is None or relation is None:
            continue
        old_fields = {f.name for f in old_edge.properties}
        new_fields = {f.name for f in new_edge.properties}
        gained = sorted(new_fields - old_fields)
        lost = sorted(old_fields - new_fields)
        if gained:
            additions.setdefault(relation, []).extend(gained)
        if lost:
            removals.setdefault(relation, []).extend(lost)

    if additions:
        ops.append(AddEdgePropertiesOp(additions=additions))
    if removals:
        ops.append(RemoveEdgePropertiesOp(removals=removals))
    return ops


def _identity_ops(
    base: GraphManifest,
    target: GraphManifest,
    hints: RenameHints,
    warnings: list[str],
) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    base_vertices = _vertices_after_renames(base, hints)
    target_vertices = _vertices(target)

    replacements: dict[str, dict[str, Any]] = {}
    secondary_add: dict[str, list[Any]] = {}
    secondary_remove: dict[str, list[str]] = {}

    for name, new_vertex in target_vertices.items():
        old_vertex = base_vertices.get(name)
        if old_vertex is None:
            continue
        if _identity_key(old_vertex) != _identity_key(new_vertex):
            replacements[name] = {
                "to": _identity_target(new_vertex),
                # The old identity's fate is already visible in the target's
                # property list, so demoting it would invent a lookup key the
                # author did not ask for.
                "retire": "keep",
            }

        old_secondary = {_secondary_key(s): s for s in old_vertex.secondary_identities}
        new_secondary = {_secondary_key(s): s for s in new_vertex.secondary_identities}
        gained = [s for key, s in new_secondary.items() if key not in old_secondary]
        lost = [
            old_secondary[key].name or ""
            for key in old_secondary
            if key not in new_secondary
        ]
        if gained:
            secondary_add[name] = gained
        if lost and all(lost):
            secondary_remove[name] = lost
        elif lost:
            warnings.append(
                f"vertex '{name}': an unnamed secondary identity was removed and "
                "cannot be addressed by name"
            )

    if replacements:
        ops.append(ReplaceIdentityOp(vertices=replacements))
    if secondary_add:
        ops.append(AddSecondaryIdentitiesOp(additions=secondary_add))
    if secondary_remove:
        ops.append(RemoveSecondaryIdentitiesOp(removals=secondary_remove))
    return ops


def _index_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints
) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    base_profile = _profile(base)
    target_profile = _profile(target)
    if base_profile is None or target_profile is None:
        return ops

    renamed = dict(hints.vertices)
    base_vertex_indexes = {
        renamed.get(name, name): indexes
        for name, indexes in base_profile.vertex_indexes.items()
    }

    added: dict[str, list[Index]] = {}
    removed: dict[str, list[list[str]]] = {}
    for name, indexes in target_profile.vertex_indexes.items():
        old = {tuple(ix.fields) for ix in base_vertex_indexes.get(name, [])}
        new_ones = [ix for ix in indexes if tuple(ix.fields) not in old]
        if new_ones:
            added[name] = new_ones
    for name, indexes in base_vertex_indexes.items():
        new = {tuple(ix.fields) for ix in target_profile.vertex_indexes.get(name, [])}
        gone = [list(ix.fields) for ix in indexes if tuple(ix.fields) not in new]
        if gone:
            removed[name] = gone

    if added:
        ops.append(AddVertexIndexesOp(indexes=added))
    if removed:
        ops.append(RemoveVertexIndexesOp(indexes=removed))

    ops += _edge_index_ops(base_profile, target_profile)
    return ops


def _edge_index_ops(base_profile: Any, target_profile: Any) -> list[ManifestOp]:
    added: list[EdgeIndexEntry] = []
    removed: list[EdgeIndexEntry] = []
    base_specs = _edge_specs(base_profile)
    target_specs = _edge_specs(target_profile)

    for key, spec in target_specs.items():
        old = base_specs.get(key)
        old_fields = (
            {tuple(ix.fields) for ix in getattr(old, "indexes", [])} if old else set()
        )
        new_ones = [
            ix
            for ix in getattr(spec, "indexes", [])
            if tuple(ix.fields) not in old_fields
        ]
        if new_ones:
            added.append(
                EdgeIndexEntry(
                    source=key[0], target=key[1], relation=key[2], indexes=new_ones
                )
            )
    for key, spec in base_specs.items():
        new = target_specs.get(key)
        new_fields = (
            {tuple(ix.fields) for ix in getattr(new, "indexes", [])} if new else set()
        )
        gone = [
            list(ix.fields)
            for ix in getattr(spec, "indexes", [])
            if tuple(ix.fields) not in new_fields
        ]
        if gone:
            removed.append(
                EdgeIndexEntry(
                    source=key[0], target=key[1], relation=key[2], fields=gone
                )
            )

    ops: list[ManifestOp] = []
    if added:
        ops.append(AddEdgeIndexesOp(edges=added))
    if removed:
        ops.append(RemoveEdgeIndexesOp(edges=removed))
    return ops


def _removal_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints
) -> list[ManifestOp]:
    """Removals run last so earlier ops still address the elements they need."""
    ops: list[ManifestOp] = []

    base_edges = _edges_after_renames(base, hints)
    target_edges = _edges(target)
    gone_relations = sorted(
        {key[2] for key in base_edges if key not in target_edges and key[2] is not None}
    )
    if gone_relations:
        ops.append(RemoveEdgesOp(relations=gone_relations))

    base_vertices = _vertices_after_renames(base, hints)
    gone_vertices = sorted(set(base_vertices) - set(_vertices(target)))
    if gone_vertices:
        ops.append(RemoveVerticesOp(names=gone_vertices))
    return ops


def _warn_unexpressed(
    base: GraphManifest,
    target: GraphManifest,
    warnings: list[str],
    hints: RenameHints,
) -> None:
    """Flag differences the op vocabulary cannot currently author."""
    base_ingestion, target_ingestion = base.ingestion_model, target.ingestion_model
    if base_ingestion is not None and target_ingestion is not None:
        base_resources = {
            hints.resources.get(r.name, r.name) for r in base_ingestion.resources
        }
        target_resources = {r.name for r in target_ingestion.resources}
        if base_resources - target_resources:
            warnings.append(
                f"resources removed ({sorted(base_resources - target_resources)}) — "
                "there is no remove_resource op; supply a rename hint or edit the "
                "ingestion block directly"
            )
        if target_resources - base_resources:
            warnings.append(
                f"resources added ({sorted(target_resources - base_resources)}) — "
                "there is no add_resource op"
            )

    if (base.bindings is None) != (target.bindings is None):
        warnings.append("the bindings block was added or removed; no op expresses that")
    elif (
        base.bindings is not None
        and target.bindings is not None
        and base.bindings.to_minimal_canonical_dict()
        != target.bindings.to_minimal_canonical_dict()
    ):
        warnings.append("the bindings block differs; no op expresses bindings edits")


# -- accessors ----------------------------------------------------------


def _vertices(manifest: GraphManifest) -> dict[str, Vertex]:
    schema = manifest.graph_schema
    if schema is None:
        return {}
    return {v.name: v for v in schema.core_schema.vertex_config.vertices}


def _edges(manifest: GraphManifest) -> dict[tuple[str, str, str | None], Edge]:
    schema = manifest.graph_schema
    if schema is None:
        return {}
    return {
        (e.source, e.target, e.relation): e
        for e in schema.core_schema.edge_config.edges
    }


def _profile(manifest: GraphManifest) -> Any:
    schema = manifest.graph_schema
    return None if schema is None else schema.db_profile


def _edge_specs(profile: Any) -> dict[tuple[str, str, str | None], Any]:
    specs = getattr(profile, "edge_specs", None) or {}
    out: dict[tuple[str, str, str | None], Any] = {}
    for key, spec in specs.items():
        if isinstance(key, tuple) and len(key) >= 3:
            out[(key[0], key[1], key[2])] = spec
    return out


def _renamed_vertex_names(manifest: GraphManifest, hints: RenameHints) -> set[str]:
    return {hints.vertices.get(name, name) for name in _vertices(manifest)}


def _vertices_after_renames(
    manifest: GraphManifest, hints: RenameHints
) -> dict[str, Vertex]:
    return {
        hints.vertices.get(name, name): vertex
        for name, vertex in _vertices(manifest).items()
    }


def _edges_after_renames(
    manifest: GraphManifest, hints: RenameHints
) -> dict[tuple[str, str, str | None], Edge]:
    out: dict[tuple[str, str, str | None], Edge] = {}
    for (source, target, relation), edge in _edges(manifest).items():
        out[
            (
                hints.vertices.get(source, source),
                hints.vertices.get(target, target),
                hints.relations.get(relation, relation) if relation else relation,
            )
        ] = edge
    return out


def _type_of(field: Any) -> Any:
    value = getattr(field, "type", None)
    return getattr(value, "value", value)


def _type_spec(field: Any) -> dict[str, Any]:
    item = getattr(field, "item_type", None)
    return {
        "type": _type_of(field),
        "item_type": getattr(item, "value", item),
    }


def _identity_key(vertex: Vertex) -> tuple:
    return (
        vertex.identity_mode,
        tuple(vertex.identity),
        tuple(vertex.hash_identity_properties),
        None
        if vertex.identity_funnel is None
        else str(vertex.identity_funnel.to_minimal_canonical_dict()),
    )


def _identity_target(vertex: Vertex) -> dict[str, Any]:
    if vertex.identity_funnel is not None:
        return {
            "mode": "funnel",
            "funnel": vertex.identity_funnel.to_dict(skip_defaults=False),
        }
    if vertex.hash_identity_properties:
        return {"mode": "hash", "hash_from": list(vertex.hash_identity_properties)}
    if vertex.assigned:
        return {"mode": "assigned"}
    if vertex.blank:
        return {"mode": "blank"}
    return {"mode": "natural", "identity": list(vertex.identity)}


def _secondary_key(secondary: Any) -> tuple:
    return (secondary.name, tuple(sorted(secondary.fields)))


def _residual(replayed: GraphManifest, target: GraphManifest) -> str:
    """A short description of what still differs after replay."""
    parts: list[str] = []
    replayed_vertices, target_vertices = _vertices(replayed), _vertices(target)
    if set(replayed_vertices) != set(target_vertices):
        parts.append(
            f"vertices {sorted(set(replayed_vertices) ^ set(target_vertices))}"
        )
    replayed_edges, target_edges = _edges(replayed), _edges(target)
    if set(replayed_edges) != set(target_edges):
        parts.append(f"edges {sorted(set(replayed_edges) ^ set(target_edges))}")
    for name, vertex in target_vertices.items():
        other = replayed_vertices.get(name)
        if other is not None and other.to_minimal_canonical_dict() != (
            vertex.to_minimal_canonical_dict()
        ):
            parts.append(f"vertex '{name}' differs")
    return "; ".join(parts) or "manifest blocks differ"


__all__ = [
    "RenameHints",
    "diff_manifests",
    "diff_manifests_verified",
]
