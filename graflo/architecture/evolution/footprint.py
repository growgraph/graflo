"""What each manifest op writes and what it reads: its footprint.

A **slot** is an addressable location in a manifest, such as
``("vertex", "person", "field", "age")``. Slots form a tree: a vertex's slot
contains its fields', identity's and grounding's, and a relation's slot
contains its edges'. The whole-manifest slot :data:`WHOLE_MANIFEST` is the root
and contains every other slot.

An op has two footprints:

* its **writes** (:func:`op_slots`): every slot it changes, including, given
  the base, what its cascade rewrites in the ingestion and bindings blocks;
* its **reads** (:func:`op_reads`): what it needs to be there and does not
  itself change -- the endpoint vertices of an edge it adds, the fields it
  keys on, the edges of the relation it addresses by name.

Two ops whose writes meet (one slot covers the other, :func:`covers`) collide.
A read is disturbed by a write **at or above** it, never beneath it
(:func:`depends_on`): an edge onto ``company`` depends on ``company`` existing
under that name, not on which fields it carries. The one exception is a
relation's edge set, ``("relation", r, MEMBERS)``, which is only read, never
written, and is also disturbed by a write of an edge directly under the
relation: an op addressed to every edge of a relation reaches whatever edges it
has when it runs.

Both are tables keyed on the op model: :data:`WRITES` and :data:`READS`. Each
is total over the ``ManifestOp`` union -- an op that reads nothing beyond what
it writes says so with an explicit entry -- and an op type with no entry is a
``TypeError``, not a guess.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterable, Sequence
from typing import Any, TypeVar

from graflo.architecture.contract.ingestion.steps import ActorConfig
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import SuperProperty
from graflo.architecture.schema.hierarchy import Hierarchy
from graflo.architecture.schema.naming import canonical_key
from graflo.architecture.schema.vertex import SuperClass

from . import ops
from .ops import ManifestOp

#: An addressable location in a manifest. Tuples so they sort and hash.
Slot = tuple[str, ...]

#: The last element of a relation's edge-set slot, ``("relation", r, "edges")``.
#: Only read, never written: see :func:`_relation_members`.
MEMBERS = "edges"

#: The slot of an op that may change anything: it contains every other slot.
WHOLE_MANIFEST: Slot = ("manifest",)

#: ``{relation name: the base relations it stands for}``, from one side's renames.
RelationAliases = dict[str, set[str]]

WriteFn = Callable[[Any, GraphManifest | None], set[Slot]]
ReadFn = Callable[[Any, GraphManifest | None, RelationAliases | None], set[Slot]]

_W = TypeVar("_W", bound=Callable[..., set[Slot]])
_R = TypeVar("_R", bound=Callable[..., set[Slot]])

#: What each op model writes, keyed on the model. Total over ``ManifestOp``.
WRITES: dict[type, WriteFn] = {}

#: What each op model reads and does not write. Total over ``ManifestOp``.
READS: dict[type, ReadFn] = {}


def _writes(*kinds: type) -> Callable[[_W], _W]:
    """Register the decorated function as the write footprint of *kinds*."""

    def register(fn: _W) -> _W:
        for kind in kinds:
            if kind in WRITES:
                raise TypeError(f"write footprint for {kind.__name__} registered twice")
            WRITES[kind] = fn
        return fn

    return register


def _reads(*kinds: type) -> Callable[[_R], _R]:
    """Register the decorated function as the read footprint of *kinds*."""

    def register(fn: _R) -> _R:
        for kind in kinds:
            if kind in READS:
                raise TypeError(f"read footprint for {kind.__name__} registered twice")
            READS[kind] = fn
        return fn

    return register


# ── slots ───────────────────────────────────────────────────────────────────


def vertex_slot(name: str) -> Slot:
    """A vertex's slot, keyed on its *canonical* name.

    Keying on ``canonical_key`` rather than the raw string matters because
    without it, one side's ``order_line`` and the
    other's ``OrderLine`` occupy different slots, merge cleanly, and produce a
    schema holding both as unrelated types with the data split between them and
    nothing raising. Detecting that is exactly what a conflict is for.
    """
    return ("vertex", "/".join(canonical_key(name)) or name.lower())


def edge_slot(source: str, target: str, relation: str | None) -> Slot:
    """An edge's slot, nested under its relation's.

    Ops address edges two ways -- by relation name (``remove_edges``,
    ``rename_relations``, ``merge_edges``) and by triple (``set_edge_directed``,
    ``retarget_edges``, ...). Containment is what makes the two families see
    each other: removing a relation covers every edge on it, so flipping one of
    those edges on the other side is a conflict rather than a clean merge that
    applies the flip to an edge the removal already deleted. An edge with no
    relation is unreachable by any relation-addressed op and keeps its own root.
    """
    endpoints = (
        "/".join(canonical_key(source)) or source.lower(),
        "/".join(canonical_key(target)) or target.lower(),
    )
    if relation is None:
        return ("edge", *endpoints)
    return (*relation_slot(relation), "edge", *endpoints)


def edge_field_slot(source: str, target: str, relation: str | None) -> Slot:
    """Where a per-edge property change lands: its relation's property slot.

    The same slot as the relation-wide form, so a change to one edge of a
    relation and a change to the whole relation see each other. An edge with
    no relation has properties under its own root.
    """
    if relation is None:
        return (*edge_slot(source, target, None), "field")
    return (*relation_slot(relation), "field")


def field_slot(vertex: str, field: str) -> Slot:
    """A vertex property's slot, under its vertex's, keyed on canonical names."""
    return (*vertex_slot(vertex), "field", "/".join(canonical_key(field)) or field)


def relation_slot(relation: str) -> Slot:
    """A relation's slot, canonicalized like every other name."""
    return ("relation", "/".join(canonical_key(relation)) or relation.lower())


def resource_slot(name: str) -> Slot:
    """A resource's slot."""
    # A resource's pipeline is an ordered program: one slot for the whole
    # resource, never one per step.
    return ("resource", name)


def describe_slot(slot: Slot) -> str:
    """A slot as a human reads it: ``vertex/person/field/age``."""
    return "/".join(slot)


def covers(outer: Slot, inner: Slot) -> bool:
    """Whether *outer* is *inner* or an ancestor of it.

    Slots form a tree -- ``("vertex", "person")`` contains
    ``("vertex", "person", "field", "age")`` and
    ``("vertex", "person", "identity")`` -- and containment is what makes a
    rename collide with an edit to the renamed thing. Comparing slots for
    *equality* alone would let one side rename ``person`` while the other adds
    a field to ``person``, merge both cleanly, and produce a change set whose
    second half addresses a vertex the first half removed.

    The whole-manifest slot is the root of the tree: it contains every slot.
    """
    if outer == WHOLE_MANIFEST:
        return True
    return len(outer) <= len(inner) and inner[: len(outer)] == outer


def depends_on(read: Slot, written: Slot) -> bool:
    """A read is disturbed by a write at or above it, not by one beneath it.

    Two exceptions. A relation's edge set (:func:`_relation_members`) is also
    disturbed by a write of an edge directly under the relation -- an edge
    added, retargeted or re-keyed there -- but not by a write beneath an edge.
    A whole name space (:data:`NAMESPACES`) is disturbed by any write in it.
    """
    if covers(written, read):
        return True
    if read in NAMESPACES:
        return covers(read, written)
    return (
        len(read) == 3
        and read[2] == MEMBERS
        and len(written) == 5
        and written[:2] == read[:2]
        and written[2] == "edge"
    )


# ── writes ──────────────────────────────────────────────────────────────────


def op_slots(op: ManifestOp, base: GraphManifest | None = None) -> set[Slot]:
    """Every addressable location *op* writes.

    With *base*, the manifest the op applies to, the footprint also holds what
    the op's cascade rewrites there: the resources whose pipeline names a type,
    relation or property it removes, renames or merges, and the bindings wiring
    them. Without it, only what the op names.

    Dispatch is on the op *class* (:data:`WRITES`), not on its ``op`` string
    literal. Two reasons: each entry is typed against its op model, so a field
    read against the wrong model is caught at check time rather than at merge
    time; and a literal renamed in the ``ops`` package cannot silently fall
    through to a catch-all.

    There is no catch-all. The table is total over the op vocabulary, which
    ``test_merge3.py`` asserts; an op type with no entry raises rather than
    being given a footprint by guess.

    Raises:
        TypeError: *op*'s type has no write footprint.
    """
    write = WRITES.get(type(op))
    if write is None:
        raise TypeError(f"no footprint for op {type(op).__name__}")
    return write(op, base)


def _meet(one: Slot, other: Slot) -> bool:
    return covers(one, other) or covers(other, one)


def wired_resources(bindings: Any) -> set[str]:
    """The resources *bindings* wire, by a connector's ``resource_name`` or a row."""
    names = {c.resource_name for c in bindings.connectors if c.resource_name}
    names |= {
        row.get("resource") if isinstance(row, dict) else row.resource
        for row in bindings.resource_connector
    }
    return {name for name in names if name}


def _bindings_wire(base: GraphManifest, resources: set[str]) -> bool:
    """Whether *base*'s bindings wire any of *resources*."""
    return base.bindings is not None and bool(
        wired_resources(base.bindings) & resources
    )


def _cascade(base: GraphManifest | None, written: set[Slot]) -> set[Slot]:
    """What a schema op's cascade rewrites in *base*'s ingestion and bindings.

    A resource is rewritten when its pipeline names a slot *written* meets
    (:func:`_pipeline_reads`); the bindings are rewritten when they wire one.
    """
    if base is None or base.ingestion_model is None:
        return set()
    trimmed = {
        resource.name
        for resource in base.ingestion_model.resources
        if any(_meet(name, w) for name in _pipeline_reads(resource) for w in written)
    }
    slots = {resource_slot(name) for name in trimmed}
    if _bindings_wire(base, trimmed):
        slots.add(("bindings",))
    return slots


def _reads_profile(fn: ReadFn) -> ReadFn:
    """Extend the read footprint of an op writing a part of the database profile.

    The part lives under its element's slot, but a whole-profile write
    (``set_db_profile``, ``sanitize``) replaces it in one order only.
    """

    @functools.wraps(fn)
    def with_profile(
        op: Any, base: GraphManifest | None, aliases: RelationAliases | None
    ) -> set[Slot]:
        return fn(op, base, aliases) | {PROFILE}

    return with_profile


def _cascading(fn: WriteFn) -> WriteFn:
    """Extend a write footprint with what its cascade rewrites in the base."""

    @functools.wraps(fn)
    def with_cascade(op: Any, base: GraphManifest | None) -> set[Slot]:
        written = fn(op, base)
        return written | _cascade(base, written)

    return with_cascade


# ── writes: vertices ────────────────────────────────────────────────────────


@_writes(ops.AddVerticesOp)
def _add_vertices_writes(
    op: ops.AddVerticesOp, base: GraphManifest | None
) -> set[Slot]:
    return {vertex_slot(v.name) for v in op.vertices}


@_writes(ops.RemoveVerticesOp)
@_cascading
def _remove_vertices_writes(
    op: ops.RemoveVerticesOp, base: GraphManifest | None
) -> set[Slot]:
    return {vertex_slot(name) for name in op.names}


@_writes(ops.RenameVerticesOp)
@_cascading
def _rename_vertices_writes(
    op: ops.RenameVerticesOp, base: GraphManifest | None
) -> set[Slot]:
    # Both names: a rename collides with any edit to either side of it.
    slots: set[Slot] = set()
    for old, new in op.renames.items():
        slots |= {vertex_slot(old), vertex_slot(new)}
    return slots


@_writes(ops.MergeVerticesOp)
@_cascading
def _merge_vertices_writes(
    op: ops.MergeVerticesOp, base: GraphManifest | None
) -> set[Slot]:
    slots = {vertex_slot(name) for name in op.sources}
    slots.add(vertex_slot(op.into))
    return slots


@_writes(ops.CanonicalizeOp)
@_cascading
def _canonicalize_writes(
    op: ops.CanonicalizeOp, base: GraphManifest | None
) -> set[Slot]:
    # Every name on either side of the map is occupied, as for a rename.
    slots: set[Slot] = set()
    for old, new in op.vertices.items():
        slots |= {vertex_slot(old), vertex_slot(new)}
    for vertex, renames in op.properties.items():
        for old, new in renames.items():
            slots |= {field_slot(vertex, old), field_slot(vertex, new)}
    for old, new in op.relations.items():
        slots |= {relation_slot(old), relation_slot(new)}
    return slots


# ── writes: vertex properties ───────────────────────────────────────────────


@_writes(ops.AddVertexPropertiesOp)
def _add_vertex_properties_writes(
    op: ops.AddVertexPropertiesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = set()
    for vertex in op.additions:
        slots |= {field_slot(vertex, name) for name in op.field_names(vertex)}
    return slots


@_writes(ops.RemoveVertexPropertiesOp)
def _remove_vertex_properties_writes(
    op: ops.RemoveVertexPropertiesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = set()
    for vertex, fields in op.removals.items():
        slots |= {field_slot(vertex, field) for field in fields}
    return slots


@_writes(ops.RenameVertexPropertiesOp)
@_cascading
def _rename_vertex_properties_writes(
    op: ops.RenameVertexPropertiesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = set()
    for vertex, renames in op.renames.items():
        for old, new in renames.items():
            slots |= {field_slot(vertex, old), field_slot(vertex, new)}
    return slots


# ── writes: identity ────────────────────────────────────────────────────────


@_writes(ops.ReplaceIdentityOp)
def _replace_identity_writes(
    op: ops.ReplaceIdentityOp, base: GraphManifest | None
) -> set[Slot]:
    # Identity is a property of the vertex as a whole, not of one field, so
    # two sides re-keying the same vertex must collide even when they name
    # entirely different fields.
    slots: set[Slot] = set()
    for vertex, replacement in op.replacements.items():
        slot = vertex_slot(vertex)
        slots.add((*slot, "identity"))
        if replacement.retire == "demote":
            # The retired key becomes a secondary identity, with its index.
            slots |= {(*slot, "secondary"), (*slot, "index")}
        current = _vertex(base, vertex)
        if current is None:
            continue
        if current.secondary_identities:
            # A lookup key the new identity restates is dropped.
            slots.add((*slot, "secondary"))
        schema = base.graph_schema if base is not None else None
        if schema is not None and vertex in schema.db_profile.vertex_indexes:
            # The old identity's indexes are dropped.
            slots.add((*slot, "index"))
        if replacement.retire == "drop":
            # The old key's fields are removed, cascade and all.
            dropped = {field_slot(vertex, field) for field in current.identity}
            slots |= dropped | _cascade(base, dropped)
        if replacement.endpoints == "pin_to_retired":
            slots |= _cascade(base, {slot})
    return slots


def _vertex(base: GraphManifest | None, name: str) -> Any:
    """The vertex *name* in *base*, if both exist."""
    if base is None or base.graph_schema is None:
        return None
    for vertex in base.graph_schema.core_schema.vertex_config.vertices:
        if vertex.name == name:
            return vertex
    return None


@_writes(ops.AddSecondaryIdentitiesOp)
def _add_secondary_identities_writes(
    op: ops.AddSecondaryIdentitiesOp, base: GraphManifest | None
) -> set[Slot]:
    # A secondary identity carries a derived index in the vertex's indexes.
    return {
        (*vertex_slot(vertex), part)
        for vertex in op.additions
        for part in ("secondary", "index")
    }


@_writes(ops.RemoveSecondaryIdentitiesOp)
def _remove_secondary_identities_writes(
    op: ops.RemoveSecondaryIdentitiesOp,
    base: GraphManifest | None,
) -> set[Slot]:
    return {
        (*vertex_slot(vertex), part)
        for vertex in op.removals
        for part in ("secondary", "index")
    }


# ── writes: edges, addressed by endpoints ───────────────────────────────────


@_writes(
    ops.AddEdgesOp,
    ops.ReplaceEdgeIdentitiesOp,
    ops.AddEdgeIndexesOp,
    ops.RemoveEdgeIndexesOp,
)
def _edge_triple_writes(
    op: ops.AddEdgesOp
    | ops.ReplaceEdgeIdentitiesOp
    | ops.AddEdgeIndexesOp
    | ops.RemoveEdgeIndexesOp,
    base: GraphManifest | None,
) -> set[Slot]:
    # `Edge.edge_id` is a property and the entry models' is a method, so
    # read the triple itself, which every edge-ish model carries.
    return {edge_slot(entry.source, entry.target, entry.relation) for entry in op.edges}


@_writes(ops.RetargetEdgesOp)
@_cascading
def _retarget_edges_writes(
    op: ops.RetargetEdgesOp, base: GraphManifest | None
) -> set[Slot]:
    # Both triples: the edge it moves away from one, and the edge it creates
    # at the other, which an edge added there by the other side collides with.
    return {
        edge_slot(*edge_id)
        for entry in op.edges
        for edge_id in (entry.edge_id(), entry.retargeted_edge_id())
    }


@_writes(ops.SetEdgeDirectedOp)
def _set_edge_directed_writes(
    op: ops.SetEdgeDirectedOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*edge_slot(*entry.edge_id()), "directed") for entry in op.edges}


# Block-level setters. One slot for the whole block, because that is what
# the op replaces -- two independent bindings edits therefore conflict,
# which is the price of the block having any op at all.


@_writes(ops.SetBindingsOp)
def _set_bindings_writes(
    op: ops.SetBindingsOp, base: GraphManifest | None
) -> set[Slot]:
    return {("bindings",)}


@_writes(ops.SetDbProfileOp)
def _set_db_profile_writes(
    op: ops.SetDbProfileOp, base: GraphManifest | None
) -> set[Slot]:
    # It replaces the whole profile, the parts kept under their elements
    # included: those it sets and, given the base, those it drops. A part the
    # other side adds is seen through that op's read of the profile.
    slots: set[Slot] = {PROFILE} | _profile_parts(op.profile)
    if base is not None and base.graph_schema is not None:
        slots |= _profile_parts(base.graph_schema.db_profile)
    return slots


def _profile_parts(profile: Any) -> set[Slot]:
    """The slots of a profile's indexes, edge specs and native inverses."""
    slots: set[Slot] = {(*vertex_slot(v), "index") for v in profile.vertex_indexes}
    slots |= {
        (*edge_slot(s.source, s.target, s.relation), SPEC) for s in profile.edge_specs
    }
    slots |= {(*relation_slot(r), "native_inverse") for r in profile.native_inverses}
    return slots


#: The database profile apart from the parts kept under their elements.
PROFILE: Slot = ("db_profile",)


#: Under an edge's slot, its physical spec in the database profile.
SPEC = "spec"


@_writes(ops.SanitizeOp)
def _sanitize_writes(op: ops.SanitizeOp, base: GraphManifest | None) -> set[Slot]:
    # Writes physical names into the profile and nothing else; what it reads
    # is every name in the schema (`_sanitize_reads`).
    return {PROFILE}


@_writes(ops.SetEdgeSemanticsOp)
def _set_edge_semantics_writes(
    op: ops.SetEdgeSemanticsOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*edge_slot(*entry.edge_id()), "semantics") for entry in op.edges}


@_writes(ops.SetEdgeDescriptionsOp)
def _set_edge_descriptions_writes(
    op: ops.SetEdgeDescriptionsOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*edge_slot(*entry.edge_id()), "description") for entry in op.edges}


@_writes(ops.SetEdgeSubpropertyOfOp)
def _set_edge_subproperty_of_writes(
    op: ops.SetEdgeSubpropertyOfOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*edge_slot(*entry.edge_id()), "subproperty_of") for entry in op.edges}


# ── writes: grounding ───────────────────────────────────────────────────────
#
# A narrower slot than the element itself: grounding a type and renaming a
# property of it are independent edits, and merging them is the ordinary
# case rather than a conflict.


@_writes(ops.SetVertexSemanticsOp)
def _set_vertex_semantics_writes(
    op: ops.SetVertexSemanticsOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*vertex_slot(name), "semantics") for name in op.semantics}


@_writes(ops.SetVertexDescriptionsOp)
def _set_vertex_descriptions_writes(
    op: ops.SetVertexDescriptionsOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*vertex_slot(name), "description") for name in op.descriptions}


@_writes(ops.SetVertexSubclassOfOp)
def _set_vertex_subclass_of_writes(
    op: ops.SetVertexSubclassOfOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*vertex_slot(name), "subclass_of") for name in op.subclass_of}


@_writes(ops.SetFieldSemanticsOp)
def _set_field_semantics_writes(
    op: ops.SetFieldSemanticsOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = set()
    for target in op.targets:
        if isinstance(target, ops.FieldSemanticsTarget):
            slots.add((*field_slot(target.vertex, target.field), "semantics"))
        else:
            slots.add(
                (*edge_slot(*target.edge_id()), "field", target.field, "semantics")
            )
    return slots


# ── writes: edges, addressed by relation name ───────────────────────────────


@_writes(ops.RemoveEdgesOp)
@_cascading
def _remove_edges_writes(
    op: ops.RemoveEdgesOp, base: GraphManifest | None
) -> set[Slot]:
    slots = {relation_slot(relation) for relation in op.relations}
    slots |= {edge_slot(*selector.edge_id()) for selector in op.edges}
    return slots


@_writes(ops.RenameRelationsOp)
@_cascading
def _rename_relations_writes(
    op: ops.RenameRelationsOp, base: GraphManifest | None
) -> set[Slot]:
    # `{old: new}` -- both names are occupied.
    slots: set[Slot] = set()
    for old, new in op.renames.items():
        slots |= {relation_slot(old), relation_slot(new)}
    return slots


@_writes(ops.AddInverseEdgesOp)
def _add_inverse_edges_writes(
    op: ops.AddInverseEdgesOp, base: GraphManifest | None
) -> set[Slot]:
    if op.relations is None:
        # Realizes whatever is paired when it runs: its reach is the inverse
        # table, every paired relation's edges and their directions, none of
        # which the op names. Conservatively, the whole manifest.
        return {WHOLE_MANIFEST}
    slots = {relation_slot(relation) for relation in op.relations}
    # The inverse edges land under each relation's declared partner.
    for relation in op.relations:
        if (inverse := _declared_inverse(base, relation)) is not None:
            slots.add(relation_slot(inverse))
    return slots


@_writes(ops.DeclareEdgeInversesOp)
def _declare_edge_inverses_writes(
    op: ops.DeclareEdgeInversesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = set()
    for relation, inverse in op.inverses.items():
        slots |= {
            (*relation_slot(relation), "inverse"),
            (*relation_slot(inverse), "inverse"),
        }
    slots |= {(*relation_slot(name), "inverse") for name in op.symmetric}
    return slots


@_writes(ops.RetractEdgeInversesOp)
def _retract_edge_inverses_writes(
    op: ops.RetractEdgeInversesOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*relation_slot(relation), "inverse") for relation in op.relations}


@_writes(ops.SetNativeInversesOp)
def _set_native_inverses_writes(
    op: ops.SetNativeInversesOp, base: GraphManifest | None
) -> set[Slot]:
    return {(*relation_slot(relation), "native_inverse") for relation in op.relations}


@_writes(ops.MergeEdgesOp)
@_cascading
def _merge_edges_writes(op: ops.MergeEdgesOp, base: GraphManifest | None) -> set[Slot]:
    slots = {relation_slot(relation) for relation in op.sources}
    slots.add(relation_slot(op.into))
    return slots


# ── writes: edge properties ─────────────────────────────────────────────────


@_writes(ops.AddEdgePropertiesOp)
def _add_edge_properties_writes(
    op: ops.AddEdgePropertiesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = {(*relation_slot(rel), "field") for rel in op.additions}
    slots |= {edge_field_slot(*entry.edge_id()) for entry in op.per_edge}
    return slots


@_writes(ops.RemoveEdgePropertiesOp)
def _remove_edge_properties_writes(
    op: ops.RemoveEdgePropertiesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = {(*relation_slot(rel), "field") for rel in op.removals}
    slots |= {edge_field_slot(*entry.edge_id()) for entry in op.per_edge}
    return slots


@_writes(ops.RenameEdgePropertiesOp)
def _rename_edge_properties_writes(
    op: ops.RenameEdgePropertiesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = {(*relation_slot(rel), "field") for rel in op.renames}
    slots |= {edge_field_slot(*entry.edge_id()) for entry in op.per_edge}
    return slots


# ── writes: vertex indexes ──────────────────────────────────────────────────


@_writes(ops.AddVertexIndexesOp, ops.RemoveVertexIndexesOp)
def _vertex_indexes_writes(
    op: ops.AddVertexIndexesOp | ops.RemoveVertexIndexesOp,
    base: GraphManifest | None,
) -> set[Slot]:
    return {(*vertex_slot(vertex), "index") for vertex in op.indexes}


# ── writes: types ───────────────────────────────────────────────────────────


@_writes(ops.ChangeFieldTypesOp)
def _change_field_types_writes(
    op: ops.ChangeFieldTypesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = set()
    for vertex, fields in op.vertices.items():
        slots |= {(*field_slot(vertex, field), "type") for field in fields}
    for relation, fields in op.edges.items():
        slots |= {
            (*relation_slot(relation), "field", str(field), "type") for field in fields
        }
    for entry in op.per_edge:
        slots |= {
            (*edge_field_slot(*entry.edge_id()), str(field), "type")
            for field in entry.fields
        }
    return slots


# ── writes: ingestion ───────────────────────────────────────────────────────


@_writes(ops.SetInverseEmissionOp)
def _set_inverse_emission_writes(
    op: ops.SetInverseEmissionOp, base: GraphManifest | None
) -> set[Slot]:
    # Steps are addressed by position, and a position is only meaningful
    # against the pipeline it was read from: one slot per resource.
    return {resource_slot(name) for name in op.steps}


@_writes(ops.AddResourceTransformsOp, ops.EnsureExtractedFieldsOp)
def _pipeline_additions_writes(
    op: ops.AddResourceTransformsOp | ops.EnsureExtractedFieldsOp,
    base: GraphManifest | None,
) -> set[Slot]:
    # A pipeline is one slot per resource: an ordered program cannot be
    # half-merged, so it conflicts as a unit or merges as a unit.
    slots = {resource_slot(name) for name in op.additions}
    if isinstance(op, ops.AddResourceTransformsOp):
        slots |= _registry_writes(op.transforms, base)
    return slots


#: The ingestion block apart from its resources: the transform registry.
INGESTION: Slot = ("ingestion",)


def _registry_writes(
    transforms: Sequence[Any], base: GraphManifest | None
) -> set[Slot]:
    """The registry slot, when *transforms* adds an entry *base*'s registry lacks.

    Without *base* there is nothing to compare with, so nothing.
    """
    if not transforms or base is None:
        return set()
    if base.ingestion_model is None:
        return {INGESTION}
    known = base.ingestion_model.transforms_by_name
    added = any(
        transform.name is None
        or transform.name not in known
        or known[transform.name].to_dict(skip_defaults=False)
        != transform.to_dict(skip_defaults=False)
        for transform in transforms
    )
    return {INGESTION} if added else set()


def _wiring_writes(base: GraphManifest | None, resources: set[str]) -> set[Slot]:
    """The bindings slot, when *base*'s bindings wire one of *resources*."""
    if base is not None and _bindings_wire(base, resources):
        return {("bindings",)}
    return set()


@_writes(ops.RenameResourcesOp)
def _rename_resources_writes(
    op: ops.RenameResourcesOp, base: GraphManifest | None
) -> set[Slot]:
    slots: set[Slot] = set()
    for old, new in op.renames.items():
        slots |= {resource_slot(old), resource_slot(new)}
    return slots | _wiring_writes(base, set(op.renames))


@_writes(ops.AddResourcesOp, ops.ReplaceResourcesOp)
def _resources_writes(
    op: ops.AddResourcesOp | ops.ReplaceResourcesOp, base: GraphManifest | None
) -> set[Slot]:
    slots = {resource_slot(resource.name) for resource in op.resources}
    slots |= _registry_writes(op.transforms, base)
    if (
        isinstance(op, ops.AddResourcesOp)
        and base is not None
        and base.ingestion_model is None
    ):
        slots.add(INGESTION)  # creates the block
    return slots


@_writes(ops.RemoveResourcesOp)
def _remove_resources_writes(
    op: ops.RemoveResourcesOp, base: GraphManifest | None
) -> set[Slot]:
    slots = {resource_slot(name) for name in op.names}
    return slots | _wiring_writes(base, set(op.names))


# ── writes: whole-manifest ops ──────────────────────────────────────────────


@_writes(ops.ProjectManifestOp, ops.MergeManifestsOp)
def _whole_manifest_writes(
    op: ops.ProjectManifestOp | ops.MergeManifestsOp,
    base: GraphManifest | None,
) -> set[Slot]:
    # These rewrite everything, so they conflict with any other change.
    # There is no way to merge "keep only these vertices" with an
    # unrelated edit and be sure of the result.
    return {WHOLE_MANIFEST}


# ── what an op depends on ───────────────────────────────────────────────────

_ENDPOINT_TOKENS = frozenset({"source", "target", "relation"})


def relation_aliases(side_ops: list[ManifestOp]) -> RelationAliases:
    """The base relations each name a side's renames and merges introduce stands for.

    An op after a rename addresses the new name, which the base does not know;
    its endpoints are those of the relation it was renamed from.
    """
    aliases: RelationAliases = {}

    def carry(old: str, new: str) -> None:
        aliases.setdefault(new, set()).update(aliases.get(old, {old}))

    for op in side_ops:
        if isinstance(op, ops.RenameRelationsOp):
            for old, new in op.renames.items():
                carry(old, new)
        elif isinstance(op, ops.CanonicalizeOp):
            for old, new in op.relations.items():
                carry(old, new)
        elif isinstance(op, ops.MergeEdgesOp):
            for source in op.sources:
                carry(source, op.into)
    return aliases


def _per_edge_reads(entries: Iterable[ops.EdgeSelector]) -> set[Slot]:
    """The edge triples *entries* name, and the vertices they connect."""
    return {
        vertex_slot(name) for entry in entries for name in (entry.source, entry.target)
    } | {edge_slot(*entry.edge_id()) for entry in entries}


def _relation_endpoints(
    base: GraphManifest | None,
    relation: str,
    aliases: RelationAliases | None = None,
) -> set[Slot]:
    """The edges of *relation*, or the relations it stands for, in *base*, and their vertices.

    The edges themselves are read too: removing the relation's last edge by
    triple leaves no relation for the op to address.
    """
    if base is None or base.graph_schema is None:
        return set()
    wanted = {
        relation_slot(name) for name in {relation, *(aliases or {}).get(relation, ())}
    }
    edges = [
        edge
        for edge in base.graph_schema.core_schema.edge_config.edges
        if edge.relation is not None and relation_slot(edge.relation) in wanted
    ]
    return {
        vertex_slot(name) for edge in edges for name in (edge.source, edge.target)
    } | {edge_slot(edge.source, edge.target, edge.relation) for edge in edges}


def _relation_members(
    relation: str, aliases: RelationAliases | None = None
) -> set[Slot]:
    """The edge set of *relation*, or the relations it stands for.

    An op addressed to every edge of a relation reaches whatever edges the
    relation has when it runs, so it depends on that set: an edge the other
    side adds to the relation would be reached in one order and not the other.
    :func:`depends_on` lets any write of an edge directly under the relation
    disturb this read.
    """
    return {
        (*relation_slot(name), MEMBERS)
        for name in {relation, *(aliases or {}).get(relation, ())}
    }


def _declared_inverse(base: GraphManifest | None, relation: str) -> str | None:
    """The inverse *relation* is paired with in *base*, if any."""
    if base is None or base.graph_schema is None:
        return None
    return base.graph_schema.core_schema.edge_config.inverse_of(relation)


def _key_field_reads(vertex: str, fields: list[str]) -> set[Slot]:
    """A keying field, and its type: an identity field may not become a list."""
    reads: set[Slot] = set()
    for field in fields:
        reads |= {field_slot(vertex, field), (*field_slot(vertex, field), "type")}
    return reads


def _identity_target_fields(target: Any) -> list[str]:
    if isinstance(target, ops.NaturalIdentityTarget):
        return list(target.identity)
    if isinstance(target, ops.HashIdentityTarget):
        return list(target.hash_from)
    if isinstance(target, ops.FunnelIdentityTarget):
        return [
            field
            for branch in target.funnel.branches
            for field in [*branch.fields, *(branch.when_all_present or [])]
        ]
    return []


# A hierarchy edit is valid or not by what the hierarchy entails, which runs
# through every link of a chain, not just the one named. So it reads the
# ``subclass_of`` (and ``subproperty_of``) slot of each element on the chain
# in *base*: a write there on the other side -- a link cut, a cycle closed --
# is a conflict, not a merged result that no longer validates.


def _hierarchy(base: GraphManifest | None) -> Hierarchy | None:
    schema = base.graph_schema if base is not None else None
    return Hierarchy.of(schema.core_schema) if schema is not None else None


def _class_chain_reads(names: Iterable[str], hierarchy: Hierarchy | None) -> set[Slot]:
    chain = set(names)
    if hierarchy is not None:
        chain |= {p for name in list(chain) for p in hierarchy.superclasses(name)}
    return {(*vertex_slot(name), "subclass_of") for name in chain}


def _superclass_reads(
    parents: Iterable[SuperClass], base: GraphManifest | None
) -> set[Slot]:
    local = [p.vertex for p in parents if p.vertex is not None]
    return {vertex_slot(name) for name in local} | _class_chain_reads(
        local, _hierarchy(base)
    )


def _superproperty_reads(
    child: EdgeId, parents: Iterable[SuperProperty], base: GraphManifest | None
) -> set[Slot]:
    local = [p.edge.edge_id() for p in parents if p.edge is not None]
    if not local:
        return set()
    hierarchy = _hierarchy(base)
    chain = set(local)
    if hierarchy is not None:
        chain |= {p for edge_id in local for p in hierarchy.superproperties(edge_id)}
    reads: set[Slot] = {(*edge_slot(*child), "directed")}
    for edge_id in local:
        reads |= {
            edge_slot(*edge_id),
            (*edge_slot(*edge_id), "directed"),
            vertex_slot(edge_id[0]),
            vertex_slot(edge_id[1]),
        }
    reads |= {(*edge_slot(*edge_id), "subproperty_of") for edge_id in chain}
    # Endpoint subsumption runs up the child's endpoints' class chains.
    return reads | _class_chain_reads(child[:2], hierarchy)


def _pipeline_reads(resource: Any) -> set[Slot]:
    """What a resource's pipeline names: vertices, fixed relations, mapped fields.

    A mapped field is a vertex property a step fills from a named column
    (a vertex step's ``from``, a router's ``vertex_from_map``). A property a
    step fills by its own name is not listed.
    """
    from graflo.architecture.contract.ingestion.steps.ref import iter_edge_steps

    reads = {vertex_slot(name) for name in resource.collect_vertex_names()}
    for view in iter_edge_steps(list(resource.pipeline)):
        for relation in view.relations_written() or set():
            reads.add(relation_slot(relation))
    for vertex, field in _mapped_fields(list(resource.pipeline)):
        reads.add(field_slot(vertex, field))
    return reads


def _mapped_fields(pipeline: Sequence[ActorConfig]) -> set[tuple[str, str]]:
    """``(vertex, property)`` for every property a step maps from a named column."""
    from graflo.architecture.contract.ingestion.resource import iter_steps
    from graflo.architecture.contract.ingestion.steps import (
        VertexActorConfig,
        VertexRouterActorConfig,
    )

    found: set[tuple[str, str]] = set()
    for step in iter_steps(pipeline):
        if isinstance(step, VertexActorConfig) and step.from_doc is not None:
            found |= {(step.vertex, field) for field in step.from_doc}
        elif isinstance(step, VertexRouterActorConfig) and step.vertex_from_map:
            for vertex, mapping in step.vertex_from_map.items():
                found |= {(vertex, field) for field in mapping}
    return found


def op_reads(
    op: ManifestOp,
    base: GraphManifest | None = None,
    aliases: RelationAliases | None = None,
) -> set[Slot]:
    """What *op* needs to be there and does not itself change.

    :func:`op_slots` is what an op writes. That is not enough to tell whether
    two ops are independent: ``add_edges`` writes an edge and *reads* its
    endpoint vertices, so a ``remove_vertices`` on the other side -- which
    cascades over that vertex's edges -- shares no written slot with it and
    still cannot be merged with it. One order drops the new edge without a
    word; the other does not apply.

    A read conflicts with a write **at or above** it, never below: an edge onto
    ``company`` depends on ``company`` existing under that name, not on which
    fields it carries.

    Ops addressed by relation depend on whatever that relation connects, which
    the op does not say; that is read from *base*. A relation one side renamed
    is looked up under the name *base* knows, so its endpoints are not seen --
    the rename itself occupies both names and conflicts with the other side's
    edits to the relation. *aliases* (:func:`relation_aliases` of the op's own
    side) lets an op after a rename read the endpoints of the renamed relation.

    The relation-wide forms of the edge property ops and of
    ``change_field_types`` also read the relation's edge set
    (``relation/<r>/edges``): they reach every edge the relation has when they
    run, so an edge the other side adds to the relation is a conflict. Their
    per-edge entries name their edges and do not read it.

    Dispatch is on the op class (:data:`READS`), which is total over the op
    vocabulary.

    Raises:
        TypeError: *op*'s type has no read footprint.
    """
    read = READS.get(type(op))
    if read is None:
        raise TypeError(f"no footprint for op {type(op).__name__}")
    return read(op, base, aliases)


@_reads(
    ops.RemoveVerticesOp,
    ops.RenameVerticesOp,
    ops.MergeVerticesOp,
    ops.CanonicalizeOp,
    ops.AddVertexPropertiesOp,
    ops.RemoveVertexPropertiesOp,
    ops.RenameVertexPropertiesOp,
    ops.SetVertexSemanticsOp,
    ops.SetVertexDescriptionsOp,
    ops.SetInverseEmissionOp,
    ops.AddResourceTransformsOp,
    ops.EnsureExtractedFieldsOp,
    ops.RenameResourcesOp,
    ops.RemoveResourcesOp,
    ops.ProjectManifestOp,
    ops.MergeManifestsOp,
)
def _reads_nothing(
    op: ManifestOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    """These ops read nothing beyond what they write.

    A property op lives under its vertex's slot, so the containment of written
    slots already ties it to that vertex.
    """
    return set()


# ── reads: edges, addressed by endpoints ────────────────────────────────────
#
# A local superclass or superproperty is a reference, as an edge's endpoint is.


@_reads(ops.SetVertexSubclassOfOp)
def _set_vertex_subclass_of_reads(
    op: ops.SetVertexSubclassOfOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    return _superclass_reads(
        (parent for parents in op.subclass_of.values() for parent in parents),
        base,
    )


@_reads(ops.AddVerticesOp)
def _add_vertices_reads(
    op: ops.AddVerticesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    return _superclass_reads(
        (parent for vertex in op.vertices for parent in vertex.subclass_of), base
    )


@_reads(ops.SetEdgeSubpropertyOfOp)
def _set_edge_subproperty_of_reads(
    op: ops.SetEdgeSubpropertyOfOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for entry in op.edges:
        reads |= {vertex_slot(entry.source), vertex_slot(entry.target)}
        reads |= _superproperty_reads(entry.edge_id(), entry.subproperty_of, base)
    return reads


@_reads(ops.AddEdgesOp)
def _add_edges_reads(
    op: ops.AddEdgesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for edge in op.edges:
        reads |= {vertex_slot(edge.source), vertex_slot(edge.target)}
        if edge.by is not None:
            reads.add(vertex_slot(edge.by))
        reads |= _superproperty_reads(edge.edge_id, edge.subproperty_of, base)
    return reads


@_reads(ops.RetargetEdgesOp)
def _retarget_edges_reads(
    op: ops.RetargetEdgesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for entry in op.edges:
        reads |= {vertex_slot(entry.source), vertex_slot(entry.target)}
        reads |= {
            vertex_slot(name)
            for name in (entry.new_source, entry.new_target)
            if name is not None
        }
    return reads


@_reads(ops.SetEdgeDirectedOp, ops.SetEdgeSemanticsOp, ops.SetEdgeDescriptionsOp)
def _edge_selector_reads(
    op: ops.SetEdgeDirectedOp | ops.SetEdgeSemanticsOp | ops.SetEdgeDescriptionsOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for selector in op.edges:
        reads |= {vertex_slot(selector.source), vertex_slot(selector.target)}
        if isinstance(op, ops.SetEdgeDirectedOp) and selector.relation is not None:
            # Every edge of a relation agrees on `directed`, so the op holds
            # only against the relation's edge set as it was read.
            reads |= _relation_members(selector.relation, aliases)
    return reads


@_reads(ops.ReplaceEdgeIdentitiesOp)
def _replace_edge_identities_reads(
    op: ops.ReplaceEdgeIdentitiesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for entry in op.edges:
        reads |= {vertex_slot(entry.source), vertex_slot(entry.target)}
        tokens = {token for key in entry.identities for token in key}
        if entry.relation is not None and tokens - _ENDPOINT_TOKENS:
            reads.add((*relation_slot(entry.relation), "field"))
    return reads


@_reads(ops.AddEdgeIndexesOp, ops.RemoveEdgeIndexesOp)
@_reads_profile
def _edge_indexes_reads(
    op: ops.AddEdgeIndexesOp | ops.RemoveEdgeIndexesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for entry in op.edges:
        reads |= {vertex_slot(entry.source), vertex_slot(entry.target)}
        if entry.relation is not None:
            reads.add((*relation_slot(entry.relation), "field"))
    return reads


@_reads(ops.SetFieldSemanticsOp)
def _set_field_semantics_reads(
    op: ops.SetFieldSemanticsOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for target in op.targets:
        if isinstance(target, ops.EdgeFieldSemanticsTarget):
            reads |= {vertex_slot(target.source), vertex_slot(target.target)}
            # The property it grounds, which lives under the relation.
            reads.add((*edge_field_slot(*target.edge_id()), target.field))
    return reads


# ── reads: edges, addressed by relation name ────────────────────────────────


@_reads(ops.RemoveEdgesOp)
def _remove_edges_reads(
    op: ops.RemoveEdgesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for selector in op.edges:
        reads |= {vertex_slot(selector.source), vertex_slot(selector.target)}
    for relation in op.relations:
        reads |= _relation_endpoints(base, relation, aliases)
    return reads


@_reads(ops.AddEdgePropertiesOp)
def _add_edge_properties_reads(
    op: ops.AddEdgePropertiesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.additions:
        reads |= _relation_endpoints(base, relation, aliases)
        reads |= _relation_members(relation, aliases)
    reads |= _per_edge_reads(op.per_edge)
    return reads


@_reads(ops.RemoveEdgePropertiesOp)
def _remove_edge_properties_reads(
    op: ops.RemoveEdgePropertiesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.removals:
        reads |= _relation_endpoints(base, relation, aliases)
        reads |= _relation_members(relation, aliases)
    reads |= _per_edge_reads(op.per_edge)
    return reads


@_reads(ops.RenameEdgePropertiesOp)
def _rename_edge_properties_reads(
    op: ops.RenameEdgePropertiesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.renames:
        reads |= _relation_endpoints(base, relation, aliases)
        reads |= _relation_members(relation, aliases)
    reads |= _per_edge_reads(op.per_edge)
    return reads


@_reads(ops.RenameRelationsOp)
def _rename_relations_reads(
    op: ops.RenameRelationsOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.renames:
        reads |= _relation_endpoints(base, relation, aliases)
    return reads


@_reads(ops.MergeEdgesOp)
def _merge_edges_reads(
    op: ops.MergeEdgesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in [*op.sources, op.into]:
        reads |= _relation_endpoints(base, relation, aliases)
    return reads


@_reads(ops.AddInverseEdgesOp)
def _add_inverse_edges_reads(
    op: ops.AddInverseEdgesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.relations or []:
        reads |= _relation_endpoints(base, relation, aliases)
        # It adds edges to the inverse relation, unless one is there already.
        inverse = _declared_inverse(base, relation)
        if inverse is not None:
            reads |= _relation_endpoints(base, inverse, aliases)
            reads |= _relation_members(inverse, aliases)
    return reads


@_reads(ops.SetNativeInversesOp)
@_reads_profile
def _set_native_inverses_reads(
    op: ops.SetNativeInversesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.relations or []:
        reads |= _relation_endpoints(base, relation, aliases)
    return reads


@_reads(ops.RetractEdgeInversesOp)
def _retract_edge_inverses_reads(
    op: ops.RetractEdgeInversesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.relations:
        reads |= _relation_endpoints(base, relation, aliases)
    return reads


@_reads(ops.DeclareEdgeInversesOp)
def _declare_edge_inverses_reads(
    op: ops.DeclareEdgeInversesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in [*op.inverses, *op.inverses.values(), *op.symmetric]:
        reads |= _relation_endpoints(base, relation, aliases)
        # A pair is refused unless its relations have edges.
        reads |= _relation_members(relation, aliases)
    return reads


@_reads(ops.RemoveVertexIndexesOp, ops.RemoveSecondaryIdentitiesOp)
@_reads_profile
def _profile_part_reads(
    op: ops.RemoveVertexIndexesOp | ops.RemoveSecondaryIdentitiesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    return set()


@_reads(ops.SetBindingsOp)
def _set_bindings_reads(
    op: ops.SetBindingsOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    # A binding is refused unless the resource it wires exists.
    if op.bindings is None:
        return set()
    return {resource_slot(name) for name in wired_resources(op.bindings)}


def _profile_names(profile: Any) -> set[Slot]:
    """The vertex, field, edge and relation slots a database profile names."""
    names: set[Slot] = {vertex_slot(vertex) for vertex in profile.vertex_storage_names}
    for vertex, renames in profile.vertex_property_names.items():
        names |= {vertex_slot(vertex)} | {field_slot(vertex, f) for f in renames}
    for vertex, indexes in profile.vertex_indexes.items():
        names.add(vertex_slot(vertex))
        names |= {field_slot(vertex, f) for index in indexes for f in index.fields}
    for spec in profile.edge_specs:
        edge = (spec.source, spec.target, spec.relation)
        fields = {f for index in spec.indexes for f in index.fields}
        fields |= set(spec.property_names)
        names |= {edge_slot(*edge), vertex_slot(spec.source), vertex_slot(spec.target)}
        names |= {(*edge_field_slot(*edge), f) for f in fields}
    defaults = profile.default_property_values
    if defaults is not None:
        for vertex, values in defaults.vertices.items():
            names |= {vertex_slot(vertex)} | {field_slot(vertex, f) for f in values}
        for entry in defaults.edges:
            edge = (entry.source, entry.target, entry.relation)
            names |= {
                edge_slot(*edge),
                vertex_slot(entry.source),
                vertex_slot(entry.target),
            }
            names |= {(*edge_field_slot(*edge), f) for f in entry.values}
    names |= {relation_slot(relation) for relation in profile.native_inverses}
    return names


@_reads(ops.SetDbProfileOp)
def _set_db_profile_reads(
    op: ops.SetDbProfileOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    return _profile_names(op.profile)


#: The name spaces of a schema: every vertex, relation and relation-less edge.
#: A read of one depends on every name beneath it (:func:`depends_on`).
NAMESPACES: tuple[Slot, ...] = (("vertex",), ("relation",), ("edge",))


@_reads(ops.SanitizeOp)
def _sanitize_reads(
    op: ops.SanitizeOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    # The stored names it records depend on every name present -- a reserved
    # word, or a collision within a namespace -- so any type, relation or
    # property added, renamed or removed beside it changes what it records.
    return set(NAMESPACES)


@_reads(ops.ChangeFieldTypesOp)
def _change_field_types_reads(
    op: ops.ChangeFieldTypesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for relation in op.edges:
        reads |= _relation_endpoints(base, relation, aliases)
        reads |= _relation_members(relation, aliases)
    reads |= _per_edge_reads(op.per_edge)
    return reads


# ── reads: keys ─────────────────────────────────────────────────────────────


@_reads(ops.ReplaceIdentityOp)
@_reads_profile
def _replace_identity_reads(
    op: ops.ReplaceIdentityOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for vertex, replacement in op.replacements.items():
        reads |= _key_field_reads(vertex, _identity_target_fields(replacement.to))
    return reads


@_reads(ops.AddSecondaryIdentitiesOp)
@_reads_profile
def _add_secondary_identities_reads(
    op: ops.AddSecondaryIdentitiesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for vertex, entries in op.additions.items():
        for entry in entries:
            reads |= _key_field_reads(vertex, list(entry.fields))
    return reads


@_reads(ops.AddVertexIndexesOp)
@_reads_profile
def _add_vertex_indexes_reads(
    op: ops.AddVertexIndexesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for vertex, indexes in op.indexes.items():
        for index in indexes:
            reads |= {field_slot(vertex, field) for field in index.fields}
    return reads


# ── reads: ingestion ────────────────────────────────────────────────────────


@_reads(ops.AddResourcesOp, ops.ReplaceResourcesOp)
def _resources_reads(
    op: ops.AddResourcesOp | ops.ReplaceResourcesOp,
    base: GraphManifest | None,
    aliases: RelationAliases | None,
) -> set[Slot]:
    reads: set[Slot] = set()
    for resource in op.resources:
        reads |= _pipeline_reads(resource)
        # A property rename rewrites every pipeline producing its vertex.
        for vertex in resource.collect_vertex_names():
            if (current := _vertex(base, vertex)) is not None:
                reads |= {field_slot(vertex, f) for f in current.property_names}
    if (
        isinstance(op, ops.AddResourcesOp)
        and base is not None
        and base.ingestion_model is None
    ):
        # Creating the block is refused while the bindings wire a resource it lacks.
        reads.add(("bindings",))
    return reads


# ── independence ────────────────────────────────────────────────────────────


def ops_independent(
    one: ManifestOp, other: ManifestOp, base: GraphManifest | None = None
) -> bool:
    """Whether *one* and *other* can be applied in either order to one effect.

    Neither writes where the other writes, and neither writes what the other
    reads, both footprints taken against *base*. Two ops reading the same thing
    are independent: two edges onto one vertex do not get in each other's way.
    """
    one_writes, other_writes = op_slots(one, base), op_slots(other, base)
    if any(covers(a, b) or covers(b, a) for a in one_writes for b in other_writes):
        return False
    return not any(
        depends_on(read, written)
        for reader, writes in ((one, other_writes), (other, one_writes))
        for read in op_reads(reader, base)
        for written in writes
    )


__all__ = [
    "INGESTION",
    "MEMBERS",
    "NAMESPACES",
    "PROFILE",
    "READS",
    "SPEC",
    "WHOLE_MANIFEST",
    "WRITES",
    "RelationAliases",
    "Slot",
    "covers",
    "depends_on",
    "describe_slot",
    "edge_field_slot",
    "edge_slot",
    "field_slot",
    "op_reads",
    "op_slots",
    "ops_independent",
    "relation_aliases",
    "relation_slot",
    "resource_slot",
    "vertex_slot",
]
