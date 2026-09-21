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

import json
import logging
from typing import Any

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgePhysicalKey, Index
from graflo.architecture.schema.database_features import (
    DatabaseProfile,
    EdgePhysicalSpec,
)
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Field, Vertex

from .canonicalize import canonical_payload
from .ops import (
    AddEdgeIndexesOp,
    AddEdgePropertiesOp,
    AddEdgesOp,
    AddResourcesOp,
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexIndexesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    ChangeFieldTypesOp,
    DeclareEdgeInversesOp,
    EdgeFieldSemanticsTarget,
    EdgeIndexEntry,
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
    ReplaceIdentityOp,
    RetractEdgeInversesOp,
    SetBindingsOp,
    SetDbProfileOp,
    SetEdgeDirectedOp,
    SetEdgeSemanticsOp,
    SetFieldSemanticsOp,
    SetNativeInversesOp,
    SetVertexDescriptionsOp,
    SetVertexSemanticsOp,
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
            merge_hint="MergeManifestsOp with explicit resource_renames",
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

    rename_ops = _rename_ops(hints)
    renamed = _apply_renames(base, rename_ops)
    if renamed is not None:
        # Every later stage compares against the manifest the renames actually
        # produce, rather than re-deriving each rename's reach by hand: a rename
        # rewrites resource pipelines, secondary identities and index keys, and
        # a stage that compared against the pre-rename base would report each
        # of those as an edit -- or re-add what the rename already moved.
        base, hints = renamed, RenameHints()

    profile_replaced = _profile_needs_replacing(base, target)
    ops += rename_ops
    ops += _inverse_withdrawal_ops(base, target, hints)
    ops += _vertex_structure_ops(base, target, hints, warnings)
    ops += _edge_structure_ops(base, target, hints, warnings)
    ops += _vertex_property_ops(base, target, hints)
    ops += _edge_property_ops(base, target, hints, warnings)
    ops += _identity_ops(base, target, hints, warnings)
    ops += _semantics_ops(base, target, hints)
    ops += _inverse_declaration_ops(
        base, target, hints, profile_replaced=profile_replaced
    )
    if not profile_replaced:
        # A profile replacement carries the indexes too, so emitting both would
        # be redundant and order-sensitive.
        ops += _index_ops(base, target, hints)
    ops += _resource_ops(base, target, hints, warnings)
    ops += _removal_ops(base, target, hints)
    ops += _block_ops(base, target, profile_replaced=profile_replaced)

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


def _apply_renames(
    base: GraphManifest, rename_ops: list[ManifestOp]
) -> GraphManifest | None:
    """*base* with *rename_ops* applied, or ``None`` when there are none or they fail.

    A failing rename is left for verification to report; the stages then fall
    back to reading the hints against the unrenamed base.
    """
    if not rename_ops:
        return None
    from .apply import apply_evolution

    try:
        return apply_evolution(base, rename_ops, bump_version=False, finish_init=False)
    except Exception:
        logger.debug("rename hints do not apply to the base", exc_info=True)
        return None


def _rename_ops(hints: RenameHints) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    if hints.vertices:
        ops.append(RenameVerticesOp(renames=dict(hints.vertices)))
    if hints.relations:
        ops.append(RenameRelationsOp(renames=dict(hints.relations)))
    if hints.resources:
        ops.append(RenameResourcesOp(renames=dict(hints.resources)))
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


def _declared_inverses(
    manifest: GraphManifest, relations: dict[str, str] | None = None
) -> set[frozenset[str]]:
    """Declared pairs and symmetric relations as unordered name sets, renamed by *relations*."""
    schema = manifest.graph_schema
    if schema is None:
        return set()
    edge_config = schema.core_schema.edge_config
    rename = relations or {}
    return {
        frozenset(rename.get(name, name) for name in (pair.relation, pair.inverse))
        for pair in edge_config.inverses
    } | {frozenset((rename.get(name, name),)) for name in edge_config.symmetric}


def _native_inverses(
    manifest: GraphManifest, relations: dict[str, str] | None = None
) -> set[str]:
    profile = _profile(manifest)
    if profile is None:
        return set()
    rename = relations or {}
    return {rename.get(name, name) for name in profile.native_inverses}


def _inverse_withdrawal_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints
) -> list[ManifestOp]:
    """Withdraw native inverses, then retract declarations, before any edge changes.

    A declaration constrains ``directed`` (a pair forbids undirected edges, a
    symmetric relation requires them) and a native inverse pins its pair, so
    both go before ``set_edge_directed`` and removals can trip over them.
    """
    ops: list[ManifestOp] = []
    withdrawn = sorted(
        _native_inverses(base, hints.relations) - _native_inverses(target)
    )
    if withdrawn:
        ops.append(SetNativeInversesOp(relations=withdrawn, enabled=False))
    retracted = sorted(
        min(declared)
        for declared in _declared_inverses(base, hints.relations)
        - _declared_inverses(target)
    )
    if retracted:
        ops.append(RetractEdgeInversesOp(relations=retracted))
    return ops


def _inverse_declaration_ops(
    base: GraphManifest,
    target: GraphManifest,
    hints: RenameHints,
    *,
    profile_replaced: bool,
) -> list[ManifestOp]:
    """Declare, then enable native inverses, once the edges they name are in place.

    Enabling is left to ``set_db_profile`` when the profile is replaced wholesale
    anyway.
    """
    ops: list[ManifestOp] = []
    new = _declared_inverses(target) - _declared_inverses(base, hints.relations)
    pairs = {min(names): max(names) for names in new if len(names) == 2}
    symmetric = sorted(next(iter(names)) for names in new if len(names) == 1)
    if pairs or symmetric:
        ops.append(
            DeclareEdgeInversesOp(
                inverses=dict(sorted(pairs.items())), symmetric=symmetric
            )
        )
    enabled = sorted(_native_inverses(target) - _native_inverses(base, hints.relations))
    if enabled and not profile_replaced:
        ops.append(SetNativeInversesOp(relations=enabled))
    return ops


def _vertex_property_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints
) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    base_vertices = _vertices_after_renames(base, hints)
    target_vertices = _vertices(target)

    additions: dict[str, list[str | Field]] = {}
    removals: dict[str, list[str]] = {}
    type_changes: dict[str, dict[str, dict[str, Any]]] = {}

    for name, new_vertex in target_vertices.items():
        old_vertex = base_vertices.get(name)
        if old_vertex is None:
            continue
        old_fields = {f.name: f for f in old_vertex.properties}
        new_fields = {f.name: f for f in new_vertex.properties}

        # A gained field is emitted as authored, so its type and grounding
        # replay with it rather than being dropped to a bare name.
        gained = [
            _field_entry(new_fields[f]) for f in new_fields if f not in old_fields
        ]
        lost = [f for f in old_fields if f not in new_fields]
        if gained:
            additions[name] = gained
        if lost:
            removals[name] = lost

        changed = _field_type_changes(old_fields, new_fields)
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
    base: GraphManifest,
    target: GraphManifest,
    hints: RenameHints,
    warnings: list[str],
) -> list[ManifestOp]:
    """Edge property additions, removals and retypes, per relation.

    The edge property ops address a *relation* and apply to every edge on it,
    while the diff sees each ``(source, target, relation)`` edge on its own.
    A change is expressible only when the sibling edges of a relation agree on
    it; a change that holds on some siblings and not others is reported rather
    than emitted, since a relation-wide op would apply it to every sibling.
    """
    ops: list[ManifestOp] = []
    base_edges = _edges_after_renames(base, hints)
    target_edges = _edges(target)

    by_relation: dict[str, list[tuple[Edge, Edge]]] = {}
    for key, new_edge in target_edges.items():
        old_edge = base_edges.get(key)
        if old_edge is None:
            continue
        relation = key[2]
        if relation is None:
            if _edge_fields_differ(old_edge, new_edge):
                warnings.append(
                    f"edge {key[:2]} has no relation, and the edge property ops "
                    "address a relation; its property changes are not expressed"
                )
            continue
        by_relation.setdefault(relation, []).append((old_edge, new_edge))

    additions: dict[str, list[str | Field]] = {}
    removals: dict[str, list[str]] = {}
    type_changes: dict[str, dict[str, dict[str, Any]]] = {}
    for relation, pairs in sorted(by_relation.items()):
        new_names = [{f.name for f in new.properties} for _, new in pairs]
        gained: dict[str, Field] = {}
        lost: set[str] = set()
        for old, new in pairs:
            old_names = {f.name for f in old.properties}
            for field in new.properties:
                if field.name not in old_names:
                    gained.setdefault(field.name, field)
            lost |= old_names - {f.name for f in new.properties}

        # Present on some old sibling => not an addition for this relation:
        # add_edge_properties addresses every edge under the relation, and
        # applying it would redeclare the property on the edge that has it.
        old_any = {f.name for old, _ in pairs for f in old.properties}
        added = sorted(
            n
            for n in gained
            if n not in old_any and all(n in names for names in new_names)
        )
        removed = sorted(n for n in lost if all(n not in names for names in new_names))
        for name in sorted((set(gained) - set(added)) | (lost - set(removed))):
            warnings.append(
                f"relation {relation!r}: property {name!r} differs between sibling "
                "edges; add/remove_edge_properties address a relation, not one edge"
            )
        if added:
            additions[relation] = [_field_entry(gained[n]) for n in added]
        if removed:
            removals[relation] = removed

        changed: dict[str, dict[str, Any]] = {}
        disagreeing: set[str] = set()
        for old, new in pairs:
            per_pair = _field_type_changes(
                {f.name: f for f in old.properties}, {f.name: f for f in new.properties}
            )
            for field, spec in per_pair.items():
                prior = changed.get(field)
                if prior is not None and prior != spec:
                    disagreeing.add(field)
                changed[field] = spec
        for field in sorted(disagreeing):
            warnings.append(
                f"relation {relation!r}: property {field!r} changes to different "
                "types on sibling edges; change_field_types addresses a relation"
            )
            changed.pop(field)
        if changed:
            type_changes[relation] = changed

    if additions:
        ops.append(AddEdgePropertiesOp(additions=additions))
    if type_changes:
        ops.append(ChangeFieldTypesOp(edges=type_changes))
    if removals:
        ops.append(RemoveEdgePropertiesOp(removals=removals))
    return ops


def _edge_fields_differ(old: Edge, new: Edge) -> bool:
    return {_field_key(f) for f in old.properties} != {
        _field_key(f) for f in new.properties
    }


def _field_key(field: Field) -> tuple[str, str | None, str | None]:
    """Name plus the whole type spec, for set comparison of property lists."""
    spec = _type_spec(field)
    return (field.name, spec["type"], spec["item_type"])


def _field_entry(field: Field) -> str | Field:
    """The field as authored when it carries more than a name, else the name."""
    return field if set(field.to_dict(skip_defaults=True)) - {"name"} else field.name


def _field_type_changes(
    old_fields: dict[str, Field], new_fields: dict[str, Field]
) -> dict[str, dict[str, Any]]:
    """Type specs for the fields present on both sides whose type changed.

    Compared on the whole spec rather than on ``type``: ``LIST<STRING>`` and
    ``LIST<INT>`` share a ``type`` and rewrite every stored value, so a
    ``type``-only comparison emits no op for the change that needs one most.
    """
    return {
        field: _type_spec(new_fields[field])
        for field in new_fields
        if field in old_fields
        and _type_spec(old_fields[field]) != _type_spec(new_fields[field])
    }


def _semantics_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints
) -> list[ManifestOp]:
    """Grounding changes on vertices, their properties, and edges.

    Runs after the property stages so every property it addresses exists.
    ``set_edge_semantics`` carries one value per selection, so edges are
    grouped by their target grounding and one op is emitted per distinct value.
    """
    ops: list[ManifestOp] = []
    base_vertices = _vertices_after_renames(base, hints)

    vertex_semantics: dict[str, Any] = {}
    vertex_descriptions: dict[str, str | None] = {}
    field_targets: list[FieldSemanticsTarget | EdgeFieldSemanticsTarget] = []
    for name, new_vertex in _vertices(target).items():
        old_vertex = base_vertices.get(name)
        if old_vertex is None:
            continue
        if old_vertex.semantics != new_vertex.semantics:
            vertex_semantics[name] = new_vertex.semantics
        if old_vertex.description != new_vertex.description:
            vertex_descriptions[name] = new_vertex.description
        old_fields = {f.name: f for f in old_vertex.properties}
        for field in new_vertex.properties:
            old_field = old_fields.get(field.name)
            if old_field is not None and old_field.semantics != field.semantics:
                field_targets.append(
                    FieldSemanticsTarget(
                        vertex=name, field=field.name, semantics=field.semantics
                    )
                )
    if vertex_semantics:
        ops.append(SetVertexSemanticsOp(semantics=vertex_semantics))
    if vertex_descriptions:
        ops.append(SetVertexDescriptionsOp(descriptions=vertex_descriptions))

    base_edges = _edges_after_renames(base, hints)
    groups: dict[str, tuple[Any, list[EdgeSelector]]] = {}
    for key, new_edge in _edges(target).items():
        old_edge = base_edges.get(key)
        if old_edge is None:
            continue
        old_fields = {f.name: f for f in old_edge.properties}
        for field in new_edge.properties:
            old_field = old_fields.get(field.name)
            if old_field is not None and old_field.semantics != field.semantics:
                field_targets.append(
                    EdgeFieldSemanticsTarget(
                        source=key[0],
                        target=key[1],
                        relation=key[2],
                        field=field.name,
                        semantics=field.semantics,
                    )
                )
        if old_edge.semantics == new_edge.semantics:
            continue
        canon = (
            ""
            if new_edge.semantics is None
            else json.dumps(
                new_edge.semantics.to_dict(skip_defaults=False), sort_keys=True
            )
        )
        groups.setdefault(canon, (new_edge.semantics, []))[1].append(
            EdgeSelector(source=key[0], target=key[1], relation=key[2])
        )
    if field_targets:
        ops.append(SetFieldSemanticsOp(targets=field_targets))
    for canon in sorted(groups):
        semantics, selectors = groups[canon]
        ops.append(SetEdgeSemanticsOp(edges=selectors, semantics=semantics))
    return ops


def _canonical_registry(transforms: Any) -> list[Any]:
    """A transform registry as a set: resolved by name, so order is not an edit."""
    return sorted(
        (canonical_payload(t) for t in transforms),
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    )


def _resource_ops(
    base: GraphManifest, target: GraphManifest, hints: RenameHints, warnings: list[str]
) -> list[ManifestOp]:
    """Added resources, plus a report for edits no op expresses.

    A resource's pipeline is an ordered program, and the only op that edits one
    (``add_resource_transforms``) appends; a changed pipeline is reported
    rather than approximated. Removed resources are emitted by the removal
    stage so earlier ops still find them.
    """
    base_ingestion, target_ingestion = base.ingestion_model, target.ingestion_model
    if target_ingestion is None:
        return []
    base_resources = (
        {hints.resources.get(r.name, r.name): r for r in base_ingestion.resources}
        if base_ingestion is not None
        else {}
    )
    added = [r for r in target_ingestion.resources if r.name not in base_resources]

    def _body(resource: ResourceConfig) -> dict[str, Any]:
        # The name is what the rename hint already accounts for. Canonical, so
        # two spellings of one step -- or a reordered membership set -- are not
        # reported as a pipeline edit.
        return {k: v for k, v in canonical_payload(resource).items() if k != "name"}

    appended: dict[str, list[dict[str, Any]]] = {}
    for resource in target_ingestion.resources:
        old = base_resources.get(resource.name)
        if old is None or _body(old) == _body(resource):
            continue
        steps = _appended_root_transforms(old, resource)
        if steps is None:
            warnings.append(
                f"resource {resource.name!r} differs; no op expresses a pipeline "
                "edit beyond add_resource_transforms"
            )
        else:
            appended[resource.name] = steps
    # Transforms new to the registry ride on the op that adds the resources
    # using them. Anything else -- a removed or redefined transform, or a new
    # one no added resource brings -- has no op and is reported.
    base_transforms = list(base_ingestion.transforms) if base_ingestion else []
    base_names = {t.name for t in base_transforms}
    registered = [t for t in target_ingestion.transforms if t.name not in base_names]
    carried = registered if (added or appended) else []
    if _canonical_registry([*base_transforms, *carried]) != _canonical_registry(
        target_ingestion.transforms
    ):
        warnings.append(
            "the transform registry differs; no op expresses registry edits "
            "beyond add_resource_transforms"
        )

    ops: list[ManifestOp] = []
    # Appends go first and take the new registry entries with them: they touch
    # only resources that already exist, so the registry then holds every
    # transform an added resource's steps may name. `add_resource_transforms`
    # is already irreversible, so carrying the transforms there costs
    # `add_resources` nothing of its inverse.
    if appended:
        ops.append(AddResourceTransformsOp(additions=appended, transforms=carried))
    if added:
        ops.append(
            AddResourcesOp(resources=added, transforms=[] if appended else carried)
        )
    return ops


def _appended_root_transforms(
    old: ResourceConfig, new: ResourceConfig
) -> list[dict[str, Any]] | None:
    """The transform steps *new* appends to *old*'s root pipeline, if that is all.

    ``add_resource_transforms`` appends at the root unless told otherwise, so a
    pipeline that is the old one plus trailing transform steps -- and differs in
    nothing else -- is exactly that op. Anything else returns ``None``.
    """
    old_body = {k: v for k, v in canonical_payload(old).items() if k != "name"}
    new_body = {k: v for k, v in canonical_payload(new).items() if k != "name"}
    old_steps = old_body.pop("pipeline", [])
    new_steps = new_body.pop("pipeline", [])
    if old_body != new_body or len(new_steps) <= len(old_steps):
        return None
    if new_steps[: len(old_steps)] != old_steps:
        return None
    tail = new_steps[len(old_steps) :]
    if any(step.get("type") != "transform" for step in tail):
        return None
    return [dict(step) for step in new.pipeline[len(old_steps) :]]


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
        ops.append(ReplaceIdentityOp(replacements=replacements))
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

    # An index over a secondary identity's fields is registered by
    # `finish_init` from the identity itself, so it is not an authored index:
    # removing it is refused (the identity would re-register it) and adding it
    # is redundant. It moves with its secondary identity, which the identity
    # stage already diffs.
    base_derived = _derived_index_field_sets(base, renamed)
    target_derived = _derived_index_field_sets(target, {})

    added: dict[str, list[Index]] = {}
    removed: dict[str, list[list[str]]] = {}
    for name, indexes in target_profile.vertex_indexes.items():
        old = {tuple(ix.fields) for ix in base_vertex_indexes.get(name, [])}
        derived = target_derived.get(name, set())
        new_ones = [
            ix
            for ix in indexes
            if tuple(ix.fields) not in old and frozenset(ix.fields) not in derived
        ]
        if new_ones:
            added[name] = new_ones
    for name, indexes in base_vertex_indexes.items():
        new = {tuple(ix.fields) for ix in target_profile.vertex_indexes.get(name, [])}
        derived = base_derived.get(name, set())
        gone = [
            list(ix.fields)
            for ix in indexes
            if tuple(ix.fields) not in new and frozenset(ix.fields) not in derived
        ]
        if gone:
            removed[name] = gone

    if added:
        ops.append(AddVertexIndexesOp(indexes=added))
    if removed:
        ops.append(RemoveVertexIndexesOp(indexes=removed))

    ops += _edge_index_ops(base_profile, target_profile)
    return ops


def _derived_index_field_sets(
    manifest: GraphManifest, renamed: dict[str, str]
) -> dict[str, set[frozenset[str]]]:
    """Per vertex, the field sets whose indexes its secondary identities derive."""
    schema = manifest.graph_schema
    if schema is None:
        return {}
    return {
        renamed.get(vertex.name, vertex.name): {
            entry.field_set for entry in vertex.secondary_identities
        }
        for vertex in schema.core_schema.vertex_config.vertices
    }


def _edge_index_ops(
    base_profile: DatabaseProfile, target_profile: DatabaseProfile
) -> list[ManifestOp]:
    added: list[EdgeIndexEntry] = []
    removed: list[EdgeIndexEntry] = []
    base_specs = _edge_specs(base_profile)
    target_specs = _edge_specs(target_profile)

    for key, spec in target_specs.items():
        old = base_specs.get(key)
        old_fields = {tuple(ix.fields) for ix in old.indexes} if old else set()
        new_ones = [ix for ix in spec.indexes if tuple(ix.fields) not in old_fields]
        if new_ones:
            added.append(_edge_index_entry(key, indexes=new_ones))
    for key, spec in base_specs.items():
        new = target_specs.get(key)
        new_fields = {tuple(ix.fields) for ix in new.indexes} if new else set()
        gone = [
            list(ix.fields) for ix in spec.indexes if tuple(ix.fields) not in new_fields
        ]
        if gone:
            removed.append(_edge_index_entry(key, fields=gone))

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

    base_ingestion, target_ingestion = base.ingestion_model, target.ingestion_model
    if base_ingestion is not None and target_ingestion is not None:
        target_names = {r.name for r in target_ingestion.resources}
        gone_resources = sorted(
            {hints.resources.get(r.name, r.name) for r in base_ingestion.resources}
            - target_names
        )
        if gone_resources:
            ops.append(RemoveResourcesOp(names=gone_resources))
    return ops


def _profile_needs_replacing(base: GraphManifest, target: GraphManifest) -> bool:
    """Whether the profile differs anywhere the index ops cannot reach."""
    base_profile, target_profile = _profile(base), _profile(target)
    if base_profile is None or target_profile is None:
        return False
    return bool(
        _profile_differences_outside_the_index_ops(base_profile, target_profile)
    )


def _block_ops(
    base: GraphManifest, target: GraphManifest, *, profile_replaced: bool
) -> list[ManifestOp]:
    """Set the whole ``db_profile`` / ``bindings`` block to the target's.

    Emitted **last**, after the removal cascades. Those cascades prune profile
    and bindings entries that referenced a removed vertex, so a block set
    earlier would be partly undone by them; set afterwards it lands on exactly
    the target's block whatever the cascade did.
    """
    ops: list[ManifestOp] = []

    if profile_replaced:
        target_profile = _profile(target)
        if target_profile is not None:
            ops.append(SetDbProfileOp(profile=target_profile.model_copy(deep=True)))

    if _bindings_differ(base, target):
        ops.append(
            SetBindingsOp(
                bindings=(
                    target.bindings.model_copy(deep=True)
                    if target.bindings is not None
                    else None
                )
            )
        )

    return ops


def _bindings_differ(base: GraphManifest, target: GraphManifest) -> bool:
    if (base.bindings is None) != (target.bindings is None):
        return True
    return (
        base.bindings is not None
        and target.bindings is not None
        and base.bindings.to_minimal_canonical_dict()
        != target.bindings.to_minimal_canonical_dict()
    )


def _warn_unexpressed(
    base: GraphManifest,
    target: GraphManifest,
    warnings: list[str],
    hints: RenameHints,
) -> None:
    """Flag differences the op vocabulary cannot currently author."""
    if (base.ingestion_model is None) != (target.ingestion_model is None) and (
        target.ingestion_model is None
    ):
        warnings.append("the ingestion block was removed; no op expresses that")


def _profile_differences_outside_the_index_ops(
    base_profile: DatabaseProfile, target_profile: DatabaseProfile
) -> list[str]:
    """Profile keys that differ once the index-addressable parts are set aside.

    ``vertex_indexes`` and the ``indexes`` of each edge spec are what the index
    ops author, and ``native_inverses`` what ``set_native_inverses`` authors;
    everything else on the profile has no op yet.
    """

    authored_by_ops = {"indexes"}
    spec_defaults = EdgePhysicalSpec(source="_", target="_").to_dict(
        skip_defaults=False
    )

    def _comparable(profile: DatabaseProfile) -> dict[str, Any]:
        data = profile.to_dict(skip_defaults=False)
        data.pop("vertex_indexes", None)
        # Authored by `set_native_inverses`.
        data.pop("native_inverses", None)
        specs = []
        for spec in data.get("edge_specs") or []:
            rest = {k: v for k, v in spec.items() if k not in authored_by_ops}
            # A spec carrying nothing beyond what the ops author is, for this
            # comparison, the same as no spec: the index ops create and empty
            # exactly such specs.
            if all(
                rest.get(k) == spec_defaults.get(k)
                for k in rest
                if k not in {"source", "target", "relation"}
            ):
                continue
            specs.append(json.dumps(rest, sort_keys=True))
        data["edge_specs"] = sorted(specs)
        return data

    left, right = _comparable(base_profile), _comparable(target_profile)
    return sorted(
        key for key in set(left) | set(right) if left.get(key) != right.get(key)
    )


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


def _profile(manifest: GraphManifest) -> DatabaseProfile | None:
    schema = manifest.graph_schema
    return None if schema is None else schema.db_profile


def _edge_specs(profile: DatabaseProfile) -> dict[EdgePhysicalKey, EdgePhysicalSpec]:
    """Physical specs keyed by their full physical key, ``purpose`` included.

    Keying on the full key is what keeps a ``purpose`` variant distinct from the
    base spec of the same triple: an index diff addressed without the purpose
    would be applied to the wrong spec on replay.
    """
    return {spec.physical_key: spec for spec in profile.edge_specs}


def _edge_index_entry(
    key: EdgePhysicalKey,
    *,
    indexes: list[Index] | None = None,
    fields: list[list[str]] | None = None,
) -> EdgeIndexEntry:
    return EdgeIndexEntry(
        source=key[0],
        target=key[1],
        relation=key[2],
        purpose=key[3],
        indexes=indexes or [],
        fields=fields or [],
    )


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
