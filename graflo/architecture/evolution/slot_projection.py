"""A manifest as slots: what each slot holds, and which slots that content names.

:mod:`.footprint` says which slots an op writes. This module says what is *in*
a slot, so that a write can be checked against what an op actually changed:
each part of a manifest is placed under the slot that owns it -- a property's
type under ``vertex/<v>/field/<f>/type``, a vertex's indexes (kept in the
database profile) under ``vertex/<v>/index``, an edge's properties under its
relation's ``field`` slot, as the footprints address them.

Content also *names* other slots: an edge names its endpoint types, an
identity or an index its key fields, a subproperty its parent edge, a declared
pair its relations' edges, a resource what its pipeline names, the bindings
the resources they wire, and every part of the database profile the profile. Renaming or removing a named thing changes the
content that names it, though that content lies under another slot. A write
therefore accounts for a change to the slots it covers and to every slot whose
content names one of them (:func:`changes_outside`). That is the same rule the
three-way merge applies through reads: an op on an edge reads its endpoints,
so a rename of an endpoint on the other side is a conflict.

The block slots are coarse: the database profile apart from indexes, edge specs
and native inverses is one slot, the bindings one, each resource one.
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, NamedTuple

from graflo.architecture.contract.manifest import GraphManifest

from .footprint import (
    INGESTION,
    PROFILE,
    SPEC,
    Slot,
    _pipeline_reads,
    covers,
    edge_field_slot,
    edge_slot,
    field_slot,
    relation_slot,
    resource_slot,
    vertex_slot,
    wired_resources,
)

_ENDPOINT_TOKENS = frozenset({"source", "target", "relation"})


class SlotContent(NamedTuple):
    """One slot's content in canonical JSON, and the slots that content names."""

    content: str
    names: frozenset[Slot]


def _dump(value: Any) -> Any:
    if hasattr(value, "to_minimal_canonical_dict"):
        return value.to_minimal_canonical_dict()
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_defaults=True)
    return value


def _without(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in keys}


class _Projection:
    def __init__(self) -> None:
        self.parts: dict[Slot, list[Any]] = defaultdict(list)
        self.names: dict[Slot, set[Slot]] = defaultdict(set)

    def put(self, slot: Slot, content: Any, names: set[Slot] | None = None) -> None:
        self.parts[slot].append(content)
        self.names[slot] |= names or set()

    def result(self) -> dict[Slot, SlotContent]:
        return {
            slot: SlotContent(
                json.dumps(parts, sort_keys=True, default=str),
                frozenset(self.names[slot]),
            )
            for slot, parts in self.parts.items()
        }


def _project_vertices(out: _Projection, manifest: GraphManifest) -> None:
    assert manifest.graph_schema is not None
    vertex_config = manifest.graph_schema.core_schema.vertex_config
    for vertex in vertex_config.vertices:
        slot = vertex_slot(vertex.name)
        out.put(
            slot,
            _without(
                _dump(vertex),
                "properties",
                "identity",
                "hash_identity_properties",
                "identity_funnel",
                "assigned",
                "blank",
                "secondary_identities",
                "semantics",
                "description",
                "subclass_of",
            ),
        )
        if vertex.name in vertex_config.force_types:
            out.put(
                slot, {"force_types": _dump(vertex_config.force_types[vertex.name])}
            )
        out.put(
            (*slot, "identity"),
            {
                "identity": list(vertex.identity),
                "hash": list(vertex.hash_identity_properties),
                "funnel": _dump(vertex.identity_funnel),
                "assigned": vertex.assigned,
                "blank": vertex.blank,
            },
            {field_slot(vertex.name, name) for name in vertex.identity},
        )
        if vertex.secondary_identities:
            out.put(
                (*slot, "secondary"),
                [_dump(entry) for entry in vertex.secondary_identities],
                {
                    field_slot(vertex.name, name)
                    for entry in vertex.secondary_identities
                    for name in entry.fields
                },
            )
        if vertex.semantics is not None:
            out.put((*slot, "semantics"), _dump(vertex.semantics))
        if vertex.description is not None:
            out.put((*slot, "description"), vertex.description)
        if vertex.subclass_of:
            out.put(
                (*slot, "subclass_of"),
                [_dump(parent) for parent in vertex.subclass_of],
                {
                    vertex_slot(parent.vertex)
                    for parent in vertex.subclass_of
                    if parent.vertex is not None
                },
            )
        for prop in vertex.properties:
            field = field_slot(vertex.name, prop.name)
            payload = _dump(prop)
            out.put(field, _without(payload, "type", "item_type", "semantics"))
            typed = {k: payload[k] for k in ("type", "item_type") if k in payload}
            if typed:
                out.put((*field, "type"), typed)
            if "semantics" in payload:
                out.put((*field, "semantics"), payload["semantics"])


def _project_edges(out: _Projection, manifest: GraphManifest) -> None:
    assert manifest.graph_schema is not None
    edge_config = manifest.graph_schema.core_schema.edge_config
    for edge in edge_config.edges:
        slot = edge_slot(edge.source, edge.target, edge.relation)
        ends = {vertex_slot(edge.source), vertex_slot(edge.target)}
        fields = edge_field_slot(edge.source, edge.target, edge.relation)
        keyed = {
            (*fields, token)
            for key in edge.identities
            for token in key
            if token not in _ENDPOINT_TOKENS
        }
        out.put(
            slot,
            _without(
                _dump(edge),
                "properties",
                "directed",
                "semantics",
                "description",
                "subproperty_of",
            ),
            ends | keyed | ({vertex_slot(edge.by)} if edge.by is not None else set()),
        )
        out.put((*slot, "directed"), edge.directed, ends)
        if edge.semantics is not None:
            out.put((*slot, "semantics"), _dump(edge.semantics), ends)
        if edge.description is not None:
            out.put((*slot, "description"), edge.description, ends)
        if edge.subproperty_of:
            parents = {
                edge_slot(parent.edge.source, parent.edge.target, parent.edge.relation)
                for parent in edge.subproperty_of
                if parent.edge is not None
            }
            out.put(
                (*slot, "subproperty_of"),
                [_dump(parent) for parent in edge.subproperty_of],
                ends | parents,
            )
        # A property of an edge lives under its relation's property slot, keyed
        # by the edge, and names the edge it belongs to.
        key = json.dumps([edge.source, edge.target, edge.relation])
        for prop in edge.properties:
            payload = _dump(prop)
            out.put(
                (*fields, prop.name),
                {key: _without(payload, "type", "item_type", "semantics")},
                ends | {slot},
            )
            typed = {k: payload[k] for k in ("type", "item_type") if k in payload}
            if typed:
                out.put((*fields, prop.name, "type"), {key: typed}, ends | {slot})
            if "semantics" in payload:
                out.put((*slot, "field", prop.name, "semantics"), payload["semantics"])

    # A declaration lasts while its relations have edges, so it names their
    # edges and endpoints; a pair also names its partner's side of itself.
    def declared(relations: list[str]) -> set[Slot]:
        wanted = {relation_slot(name) for name in relations}
        names = set(wanted)
        for edge in edge_config.edges:
            if edge.relation is not None and relation_slot(edge.relation) in wanted:
                names |= {
                    edge_slot(edge.source, edge.target, edge.relation),
                    vertex_slot(edge.source),
                    vertex_slot(edge.target),
                }
        return names

    for pair in edge_config.inverses:
        payload = _dump(pair)
        relations = [value for value in payload.values() if isinstance(value, str)]
        names = declared(relations) | {
            (*relation_slot(other), "inverse") for other in relations
        }
        for relation in relations:
            out.put((*relation_slot(relation), "inverse"), payload, names)
    for relation in edge_config.symmetric:
        out.put(
            (*relation_slot(relation), "inverse"),
            {"symmetric": relation},
            declared([relation]),
        )


def _project_profile(out: _Projection, manifest: GraphManifest) -> None:
    assert manifest.graph_schema is not None
    profile = manifest.graph_schema.db_profile
    whole = {PROFILE}
    for relation in profile.native_inverses:
        out.put((*relation_slot(relation), "native_inverse"), True, whole)
    for vertex, indexes in profile.vertex_indexes.items():
        out.put(
            (*vertex_slot(vertex), "index"),
            [_dump(index) for index in indexes],
            whole | {field_slot(vertex, f) for index in indexes for f in index.fields},
        )
    for spec in profile.edge_specs:
        edge = edge_slot(spec.source, spec.target, spec.relation)
        fields = edge_field_slot(spec.source, spec.target, spec.relation)
        out.put(
            (*edge, SPEC),
            _dump(spec),
            whole
            | {edge, vertex_slot(spec.source), vertex_slot(spec.target)}
            | {(*fields, f) for index in spec.indexes for f in index.fields}
            | {(*fields, f) for f in spec.property_names},
        )
    out.put(
        PROFILE,
        _without(_dump(profile), "native_inverses", "vertex_indexes", "edge_specs"),
        {
            vertex_slot(vertex)
            for vertex in [
                *profile.vertex_storage_names,
                *profile.vertex_property_names,
            ]
        },
    )


def slot_contents(manifest: GraphManifest) -> dict[Slot, SlotContent]:
    """*manifest* as ``{slot: SlotContent}``, each part under the slot that owns it.

    Metadata (name, version) is left out, as the content hash leaves it out.
    """
    out = _Projection()
    if manifest.graph_schema is not None:
        _project_vertices(out, manifest)
        _project_edges(out, manifest)
        _project_profile(out, manifest)
    if manifest.bindings is not None:
        out.put(
            ("bindings",),
            _dump(manifest.bindings),
            {resource_slot(name) for name in wired_resources(manifest.bindings)},
        )
    if manifest.ingestion_model is not None:
        for resource in manifest.ingestion_model.resources:
            out.put(
                resource_slot(resource.name), _dump(resource), _pipeline_reads(resource)
            )
        out.put(
            INGESTION,
            _without(_dump(manifest.ingestion_model), "resources"),
        )
    return out.result()


def changes_outside(
    writes: set[Slot],
    before: dict[Slot, SlotContent],
    after: dict[Slot, SlotContent],
) -> list[Slot]:
    """The changed slots that *writes* does not account for, sorted.

    A slot whose content differs between *before* and *after* is accounted for
    when a write covers it, or covers a slot its content names before or after.
    An empty result is write soundness: the op changed nothing it did not
    declare.
    """
    unaccounted: list[Slot] = []
    for slot in sorted(set(before) | set(after)):
        old, new = before.get(slot), after.get(slot)
        if (old and old.content) == (new and new.content):
            continue
        if any(covers(write, slot) for write in writes):
            continue
        named = (old.names if old else frozenset()) | (
            new.names if new else frozenset()
        )
        if any(covers(write, name) for write in writes for name in named):
            continue
        unaccounted.append(slot)
    return unaccounted


__all__ = ["SlotContent", "changes_outside", "slot_contents"]
