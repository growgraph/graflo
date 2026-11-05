"""Three-way merge over manifest change sets, and tracked re-merges.

Merging two world models is not diffing them. Both sides descend from a common
ancestor, so the question is never "what is different" but "what did each side
*change*, and do those changes collide". That is the three-way shape: diff
base→left, diff base→right, and reconcile.

Slots
-----

Reconciliation happens per **slot** -- the addressable location an op touches,
such as ``("vertex", "person", "field", "age")``. Two sides that touch disjoint
slots, and do not change what the other depends on, merge automatically. Two sides that make the *same* change to one slot
merge to that change, once. Two sides that make different changes to one slot
are a :class:`MergeConflict`, reported rather than guessed at.

Three things make the slot the right unit:

* An **order-significant sequence is one slot**. A resource pipeline is an
  ordered program, and half-merging two edits to a program produces something
  neither author wrote. It conflicts as a unit or it merges as a unit.
* A **rename occupies both names**. Renaming ``person`` → ``customer`` on one
  side while the other side adds a field to ``person`` is a genuine collision,
  and it is invisible unless the rename is understood to touch the old slot too.
* Each side's ops are first **split one per slot** (:func:`split_by_slot`):
  the differ emits one op per kind for the whole manifest, and a decision on
  one slot must not carry another slot's edits. An op that cannot be split
  (a swap, a pipeline) is **atomic**: if any one of its slots is contested,
  the whole op is held back. Applying half an op is not a merge.
* An op **reads** as well as writes. An edge added onto ``company`` writes the
  edge and depends on ``company``; the other side removing ``company`` writes a
  different slot and still cannot be merged with it (:func:`op_reads`). A read
  is disturbed by a write at or above it, never by one beneath: the edge does
  not care which fields ``company`` carries.

A side whose change no operation expresses cannot be merged at all, because the
merge is assembled from each side's ops: :func:`merge_three_way` raises rather
than return a clean result that silently lacks it.

Three-way merge is not merge
----------------------------

Three-way merge (this module, commit kind ``merge3``) reconciles two descendants
of a **common ancestor**: names are expected to agree because both sides
inherited them, so disagreement is a conflict. Merge (``merge_manifests``, commit
kind ``merge``) joins **unrelated lineages** by declared equivalence: names are
expected to disagree, and the declaration is what reconciles them. Both produce
multi-parent commits; they are not the same operation and must not be conflated.

Determinism is a contract
-------------------------

The same inputs produce the same merged manifest, the same conflicts in the same
order, and -- through canonical hashing -- the same content hash. Auto-merged
ops are applied left-side-first in their diff order, then right-side. Nothing
here consults a set iteration order or a dict insertion order.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any

import suthing
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import SuperProperty
from graflo.architecture.schema.hierarchy import Hierarchy
from graflo.architecture.schema.naming import canonical_key
from graflo.architecture.schema.vertex import SuperClass

from . import ops
from .autogenerate import RenameHints, diff_manifests_verified
from .codec import RevisionOp, op_to_dict, ops_from_dicts, ops_to_dicts
from .hashing import manifest_hash
from .ops import ManifestOp

logger = logging.getLogger(__name__)

#: An addressable location in a manifest. Tuples so they sort and hash.
Slot = tuple[str, ...]


class MergeError(RuntimeError):
    """A merge cannot proceed: unmergeable inputs, or an invalid resolution."""


# ── slots ───────────────────────────────────────────────────────────────────


def _vertex_slot(name: str) -> Slot:
    """A vertex's slot, keyed on its *canonical* name.

    Keying on ``canonical_key`` rather than the raw string matters because
    without it, one side's ``order_line`` and the
    other's ``OrderLine`` occupy different slots, merge cleanly, and produce a
    schema holding both as unrelated types with the data split between them and
    nothing raising. Detecting that is exactly what a conflict is for.
    """
    return ("vertex", "/".join(canonical_key(name)) or name.lower())


def _edge_slot(source: str, target: str, relation: str | None) -> Slot:
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
    return (*_relation_slot(relation), "edge", *endpoints)


def _edge_field_slot(source: str, target: str, relation: str | None) -> Slot:
    """Where a per-edge property change lands: its relation's property slot.

    The same slot as the relation-wide form, so a change to one edge of a
    relation and a change to the whole relation see each other. An edge with
    no relation has properties under its own root.
    """
    if relation is None:
        return (*_edge_slot(source, target, None), "field")
    return (*_relation_slot(relation), "field")


def _field_slot(vertex: str, field: str) -> Slot:
    return (*_vertex_slot(vertex), "field", "/".join(canonical_key(field)) or field)


def _relation_slot(relation: str) -> Slot:
    """A relation's slot, canonicalized like every other name."""
    return ("relation", "/".join(canonical_key(relation)) or relation.lower())


def _resource_slot(name: str) -> Slot:
    # A resource's pipeline is an ordered program: one slot for the whole
    # resource, never one per step.
    return ("resource", name)


def op_slots(op: ManifestOp) -> set[Slot]:
    """Every addressable location *op* touches.

    Dispatch is on the op *class*, not on its ``op`` string literal. Two
    reasons: a type checker can narrow it, so a field read against the wrong op
    model is caught at check time rather than at merge time; and a literal
    renamed in the ``ops`` package cannot silently fall through to the
    catch-all here.

    Total over the op vocabulary. An op reaching the fallback is treated as
    touching the whole manifest, which conflicts with everything -- the safe
    direction, since an unrecognised op silently merging with anything is how a
    merge quietly corrupts a schema. ``test_merge3.py`` asserts every member of
    the union resolves to a real slot, so the fallback is a backstop rather
    than a policy.
    """
    slots: set[Slot] = set()

    # ── vertices ────────────────────────────────────────────────────────────
    if isinstance(op, ops.AddVerticesOp):
        slots |= {_vertex_slot(v.name) for v in op.vertices}
    elif isinstance(op, ops.RemoveVerticesOp):
        slots |= {_vertex_slot(name) for name in op.names}
    elif isinstance(op, ops.RenameVerticesOp):
        # Both names: a rename collides with any edit to either side of it.
        for old, new in op.renames.items():
            slots |= {_vertex_slot(old), _vertex_slot(new)}
    elif isinstance(op, ops.MergeVerticesOp):
        slots |= {_vertex_slot(name) for name in op.sources}
        slots.add(_vertex_slot(op.into))
    elif isinstance(op, ops.CanonicalizeOp):
        # Every name on either side of the map is occupied, as for a rename.
        for old, new in op.vertices.items():
            slots |= {_vertex_slot(old), _vertex_slot(new)}
        for vertex, renames in op.properties.items():
            for old, new in renames.items():
                slots |= {_field_slot(vertex, old), _field_slot(vertex, new)}
        for old, new in op.relations.items():
            slots |= {_relation_slot(old), _relation_slot(new)}

    # ── vertex properties ───────────────────────────────────────────────────
    elif isinstance(op, ops.AddVertexPropertiesOp):
        for vertex in op.additions:
            slots |= {_field_slot(vertex, name) for name in op.field_names(vertex)}
    elif isinstance(op, ops.RemoveVertexPropertiesOp):
        for vertex, fields in op.removals.items():
            slots |= {_field_slot(vertex, field) for field in fields}
    elif isinstance(op, ops.RenameVertexPropertiesOp):
        for vertex, renames in op.renames.items():
            for old, new in renames.items():
                slots |= {_field_slot(vertex, old), _field_slot(vertex, new)}

    # ── identity ────────────────────────────────────────────────────────────
    elif isinstance(op, ops.ReplaceIdentityOp):
        # Identity is a property of the vertex as a whole, not of one field, so
        # two sides re-keying the same vertex must collide even when they name
        # entirely different fields.
        slots |= {(*_vertex_slot(vertex), "identity") for vertex in op.replacements}
    elif isinstance(op, ops.AddSecondaryIdentitiesOp):
        slots |= {(*_vertex_slot(vertex), "secondary") for vertex in op.additions}
    elif isinstance(op, ops.RemoveSecondaryIdentitiesOp):
        slots |= {(*_vertex_slot(vertex), "secondary") for vertex in op.removals}

    # ── edges, addressed by endpoints ───────────────────────────────────────
    elif isinstance(
        op,
        (
            ops.AddEdgesOp,
            ops.RetargetEdgesOp,
            ops.ReplaceEdgeIdentitiesOp,
            ops.AddEdgeIndexesOp,
            ops.RemoveEdgeIndexesOp,
        ),
    ):
        # `Edge.edge_id` is a property and the entry models' is a method, so
        # read the triple itself, which every edge-ish model carries.
        slots |= {
            _edge_slot(entry.source, entry.target, entry.relation) for entry in op.edges
        }
    elif isinstance(op, ops.SetEdgeDirectedOp):
        slots |= {(*_edge_slot(*entry.edge_id()), "directed") for entry in op.edges}

    # Block-level setters. One slot for the whole block, because that is what
    # the op replaces -- two independent bindings edits therefore conflict,
    # which is the price of the block having any op at all.
    elif isinstance(op, ops.SetBindingsOp):
        slots |= {("bindings",)}
    elif isinstance(op, ops.SetDbProfileOp):
        slots |= {("db_profile",)}
    elif isinstance(op, ops.SanitizeOp):
        # Writes physical names into the profile and nothing else. It needs no
        # read footprint: a concurrent rename carries the recorded name along,
        # and a name the merge leaves unrecorded is filled in at write time.
        slots |= {("db_profile",)}
    elif isinstance(op, ops.SetEdgeSemanticsOp):
        slots |= {(*_edge_slot(*entry.edge_id()), "semantics") for entry in op.edges}
    elif isinstance(op, ops.SetEdgeDescriptionsOp):
        slots |= {(*_edge_slot(*entry.edge_id()), "description") for entry in op.edges}
    elif isinstance(op, ops.SetEdgeSubpropertyOfOp):
        slots |= {
            (*_edge_slot(*entry.edge_id()), "subproperty_of") for entry in op.edges
        }

    # ── grounding ───────────────────────────────────────────────────────────
    #
    # A narrower slot than the element itself: grounding a type and renaming a
    # property of it are independent edits, and merging them is the ordinary
    # case rather than a conflict.
    elif isinstance(op, ops.SetVertexSemanticsOp):
        slots |= {(*_vertex_slot(name), "semantics") for name in op.semantics}
    elif isinstance(op, ops.SetVertexDescriptionsOp):
        slots |= {(*_vertex_slot(name), "description") for name in op.descriptions}
    elif isinstance(op, ops.SetVertexSubclassOfOp):
        slots |= {(*_vertex_slot(name), "subclass_of") for name in op.subclass_of}
    elif isinstance(op, ops.SetFieldSemanticsOp):
        for target in op.targets:
            if isinstance(target, ops.FieldSemanticsTarget):
                slots.add((*_field_slot(target.vertex, target.field), "semantics"))
            else:
                slots.add(
                    (*_edge_slot(*target.edge_id()), "field", target.field, "semantics")
                )

    # ── edges, addressed by relation name ───────────────────────────────────
    elif isinstance(op, ops.RemoveEdgesOp):
        slots |= {_relation_slot(relation) for relation in op.relations}
        slots |= {_edge_slot(*selector.edge_id()) for selector in op.edges}
    elif isinstance(op, ops.RenameRelationsOp):
        # `{old: new}` -- both names are occupied.
        for old, new in op.renames.items():
            slots |= {_relation_slot(old), _relation_slot(new)}
    elif isinstance(op, ops.AddInverseEdgesOp):
        if op.relations is None:
            # Realizes whatever is declared at replay time, so it depends on the
            # whole table.
            slots.add(("edge_inverses",))
        else:
            slots |= {_relation_slot(relation) for relation in op.relations}
    elif isinstance(op, ops.DeclareEdgeInversesOp):
        for relation, inverse in op.inverses.items():
            slots |= {
                (*_relation_slot(relation), "inverse"),
                (*_relation_slot(inverse), "inverse"),
            }
        slots |= {(*_relation_slot(name), "inverse") for name in op.symmetric}
    elif isinstance(op, ops.RetractEdgeInversesOp):
        slots |= {(*_relation_slot(relation), "inverse") for relation in op.relations}
    elif isinstance(op, ops.SetNativeInversesOp):
        slots |= {
            (*_relation_slot(relation), "native_inverse") for relation in op.relations
        }
    elif isinstance(op, ops.MergeEdgesOp):
        slots |= {_relation_slot(relation) for relation in op.sources}
        slots.add(_relation_slot(op.into))

    # ── edge properties ─────────────────────────────────────────────────────
    elif isinstance(op, ops.AddEdgePropertiesOp):
        slots |= {(*_relation_slot(rel), "field") for rel in op.additions}
        slots |= {_edge_field_slot(*entry.edge_id()) for entry in op.per_edge}
    elif isinstance(op, ops.RemoveEdgePropertiesOp):
        slots |= {(*_relation_slot(rel), "field") for rel in op.removals}
        slots |= {_edge_field_slot(*entry.edge_id()) for entry in op.per_edge}
    elif isinstance(op, ops.RenameEdgePropertiesOp):
        slots |= {(*_relation_slot(rel), "field") for rel in op.renames}
        slots |= {_edge_field_slot(*entry.edge_id()) for entry in op.per_edge}

    # ── vertex indexes ──────────────────────────────────────────────────────
    elif isinstance(op, (ops.AddVertexIndexesOp, ops.RemoveVertexIndexesOp)):
        slots |= {(*_vertex_slot(vertex), "index") for vertex in op.indexes}

    # ── types ───────────────────────────────────────────────────────────────
    elif isinstance(op, ops.ChangeFieldTypesOp):
        for vertex, fields in op.vertices.items():
            slots |= {(*_field_slot(vertex, field), "type") for field in fields}
        for relation, fields in op.edges.items():
            slots |= {
                (*_relation_slot(relation), "field", str(field), "type")
                for field in fields
            }
        for entry in op.per_edge:
            slots |= {
                (*_edge_field_slot(*entry.edge_id()), str(field), "type")
                for field in entry.fields
            }

    # ── ingestion ───────────────────────────────────────────────────────────
    elif isinstance(op, ops.SetInverseEmissionOp):
        # Steps are addressed by position, and a position is only meaningful
        # against the pipeline it was read from: one slot per resource.
        slots |= {_resource_slot(name) for name in op.steps}
    elif isinstance(op, (ops.AddResourceTransformsOp, ops.EnsureExtractedFieldsOp)):
        # A pipeline is one slot per resource: an ordered program cannot be
        # half-merged, so it conflicts as a unit or merges as a unit.
        slots |= {_resource_slot(name) for name in op.additions}
    elif isinstance(op, ops.RenameResourcesOp):
        for old, new in op.renames.items():
            slots |= {_resource_slot(old), _resource_slot(new)}
    elif isinstance(op, (ops.AddResourcesOp, ops.ReplaceResourcesOp)):
        slots |= {_resource_slot(resource.name) for resource in op.resources}
    elif isinstance(op, ops.RemoveResourcesOp):
        slots |= {_resource_slot(name) for name in op.names}

    # ── whole-manifest ops ──────────────────────────────────────────────────
    elif isinstance(op, (ops.ProjectManifestOp, ops.MergeManifestsOp)):
        # These rewrite everything, so they conflict with any other change.
        # There is no way to merge "keep only these vertices" with an
        # unrelated edit and be sure of the result.
        slots.add(("manifest",))

    if not slots:
        logger.warning(
            "op '%s' has no slot mapping; treating it as touching the whole "
            "manifest, so it will conflict with any other change",
            op.op,
        )
        slots.add(("manifest",))
    return slots


# ── one op per slot ─────────────────────────────────────────────────────────

#: How an op divides into one op per slot: each payload field to split, and how.
#: ``items`` splits a list per element, ``keys`` a mapping per key, ``entries`` a
#: mapping per key *and* per element (or inner key) under it. An op kind absent
#: here is never split: a resource pipeline, an inverse table, a whole block.
_SPLITS: dict[type, tuple[tuple[str, str], ...]] = {
    ops.AddVerticesOp: (("vertices", "items"),),
    ops.RemoveVerticesOp: (("names", "items"),),
    ops.RenameVerticesOp: (("renames", "keys"),),
    ops.AddVertexPropertiesOp: (("additions", "entries"),),
    ops.RemoveVertexPropertiesOp: (("removals", "entries"),),
    ops.RenameVertexPropertiesOp: (("renames", "entries"),),
    ops.ChangeFieldTypesOp: (
        ("vertices", "entries"),
        ("edges", "entries"),
        ("per_edge", "items"),
    ),
    ops.ReplaceIdentityOp: (("replacements", "keys"),),
    ops.AddSecondaryIdentitiesOp: (("additions", "keys"),),
    ops.RemoveSecondaryIdentitiesOp: (("removals", "keys"),),
    ops.AddVertexIndexesOp: (("indexes", "keys"),),
    ops.RemoveVertexIndexesOp: (("indexes", "keys"),),
    ops.SetVertexSemanticsOp: (("semantics", "keys"),),
    ops.SetVertexDescriptionsOp: (("descriptions", "keys"),),
    ops.SetVertexSubclassOfOp: (("subclass_of", "keys"),),
    ops.SetFieldSemanticsOp: (("targets", "items"),),
    ops.AddEdgesOp: (("edges", "items"),),
    ops.RemoveEdgesOp: (("relations", "items"), ("edges", "items")),
    ops.ReplaceEdgeIdentitiesOp: (("edges", "items"),),
    ops.AddEdgeIndexesOp: (("edges", "items"),),
    ops.RemoveEdgeIndexesOp: (("edges", "items"),),
    ops.SetEdgeDirectedOp: (("edges", "items"),),
    ops.SetEdgeSemanticsOp: (("edges", "items"),),
    ops.SetEdgeDescriptionsOp: (("edges", "items"),),
    ops.SetEdgeSubpropertyOfOp: (("edges", "items"),),
    ops.RenameRelationsOp: (("renames", "keys"),),
    ops.AddEdgePropertiesOp: (("additions", "keys"), ("per_edge", "items")),
    ops.RemoveEdgePropertiesOp: (("removals", "keys"), ("per_edge", "items")),
    ops.RenameEdgePropertiesOp: (("renames", "keys"), ("per_edge", "items")),
}


def _rename_chains(op: ManifestOp) -> bool:
    """Whether a rename map renames onto a name it also renames away.

    ``{a: b, b: a}`` is a swap only as one op: split, each half collides with
    the name the other still holds.
    """
    if isinstance(op, (ops.RenameVerticesOp, ops.RenameRelationsOp)):
        maps = [op.renames]
    elif isinstance(op, ops.RenameVertexPropertiesOp):
        maps = list(op.renames.values())
    elif isinstance(op, ops.RenameEdgePropertiesOp):
        maps = [*op.renames.values(), *(entry.renames for entry in op.per_edge)]
    else:
        return False
    return any(
        {canonical_key(new) for new in renames.values()}
        & {canonical_key(old) for old in renames}
        for renames in maps
    )


def split_by_slot(op: ManifestOp) -> list[ManifestOp]:
    """*op* as one op per slot it writes, in order; ``[op]`` when it cannot split.

    The differ emits one op per kind for the whole manifest, so a single
    ``add_vertex_properties`` carries every property any vertex gained. Split,
    a conflict holds only the part at its slot, and deciding it decides nothing
    else.
    """
    shape = _SPLITS.get(type(op))
    if shape is None or _rename_chains(op):
        return [op]
    from .codec import op_from_dict

    payload = op_to_dict(op)
    split_fields = {name for name, _how in shape}
    common = {key: value for key, value in payload.items() if key not in split_fields}

    pieces: list[dict[str, Any]] = []
    for name, how in shape:
        value = payload.get(name)
        if not value:
            continue
        if how == "items":
            pieces += [{name: [item]} for item in value]
        elif how == "keys":
            pieces += [{name: {key: inner}} for key, inner in value.items()]
        else:
            for key, inner in value.items():
                if isinstance(inner, dict):
                    pieces += [{name: {key: {k: v}}} for k, v in inner.items()]
                else:
                    pieces += [{name: {key: [entry]}} for entry in inner]
    if len(pieces) <= 1:
        return [op]
    return [op_from_dict({**common, **piece}) for piece in pieces]


# ── what an op depends on ───────────────────────────────────────────────────

_ENDPOINT_TOKENS = frozenset({"source", "target", "relation"})


#: ``{relation name: the base relations it stands for}``, from one side's renames.
RelationAliases = dict[str, set[str]]


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
        _vertex_slot(name) for entry in entries for name in (entry.source, entry.target)
    } | {_edge_slot(*entry.edge_id()) for entry in entries}


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
        _relation_slot(name) for name in {relation, *(aliases or {}).get(relation, ())}
    }
    edges = [
        edge
        for edge in base.graph_schema.core_schema.edge_config.edges
        if edge.relation is not None and _relation_slot(edge.relation) in wanted
    ]
    return {
        _vertex_slot(name) for edge in edges for name in (edge.source, edge.target)
    } | {_edge_slot(edge.source, edge.target, edge.relation) for edge in edges}


def _key_field_reads(vertex: str, fields: list[str]) -> set[Slot]:
    """A keying field, and its type: an identity field may not become a list."""
    reads: set[Slot] = set()
    for field in fields:
        reads |= {_field_slot(vertex, field), (*_field_slot(vertex, field), "type")}
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
    return {(*_vertex_slot(name), "subclass_of") for name in chain}


def _superclass_reads(
    parents: Iterable[SuperClass], base: GraphManifest | None
) -> set[Slot]:
    local = [p.vertex for p in parents if p.vertex is not None]
    return {_vertex_slot(name) for name in local} | _class_chain_reads(
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
    reads: set[Slot] = {(*_edge_slot(*child), "directed")}
    for edge_id in local:
        reads |= {
            _edge_slot(*edge_id),
            (*_edge_slot(*edge_id), "directed"),
            _vertex_slot(edge_id[0]),
            _vertex_slot(edge_id[1]),
        }
    reads |= {(*_edge_slot(*edge_id), "subproperty_of") for edge_id in chain}
    # Endpoint subsumption runs up the child's endpoints' class chains.
    return reads | _class_chain_reads(child[:2], hierarchy)


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

    An op not listed reads nothing beyond what it writes: a property op lives
    under its vertex's slot, so the containment of written slots already ties
    it to that vertex.
    """
    reads: set[Slot] = set()

    # ── edges, addressed by endpoints ───────────────────────────────────────
    # A local superclass or superproperty is a reference, as an edge's endpoint is.
    if isinstance(op, ops.SetVertexSubclassOfOp):
        reads |= _superclass_reads(
            (parent for parents in op.subclass_of.values() for parent in parents),
            base,
        )
    elif isinstance(op, ops.AddVerticesOp):
        reads |= _superclass_reads(
            (parent for vertex in op.vertices for parent in vertex.subclass_of), base
        )
    elif isinstance(op, ops.SetEdgeSubpropertyOfOp):
        for entry in op.edges:
            reads |= {_vertex_slot(entry.source), _vertex_slot(entry.target)}
            reads |= _superproperty_reads(entry.edge_id(), entry.subproperty_of, base)
    elif isinstance(op, ops.AddEdgesOp):
        for edge in op.edges:
            reads |= {_vertex_slot(edge.source), _vertex_slot(edge.target)}
            if edge.by is not None:
                reads.add(_vertex_slot(edge.by))
            reads |= _superproperty_reads(edge.edge_id, edge.subproperty_of, base)
    elif isinstance(op, ops.RetargetEdgesOp):
        for entry in op.edges:
            reads |= {_vertex_slot(entry.source), _vertex_slot(entry.target)}
            reads |= {
                _vertex_slot(name)
                for name in (entry.new_source, entry.new_target)
                if name is not None
            }
    elif isinstance(
        op,
        (ops.SetEdgeDirectedOp, ops.SetEdgeSemanticsOp, ops.SetEdgeDescriptionsOp),
    ):
        for selector in op.edges:
            reads |= {_vertex_slot(selector.source), _vertex_slot(selector.target)}
    elif isinstance(op, ops.ReplaceEdgeIdentitiesOp):
        for entry in op.edges:
            reads |= {_vertex_slot(entry.source), _vertex_slot(entry.target)}
            tokens = {token for key in entry.identities for token in key}
            if entry.relation is not None and tokens - _ENDPOINT_TOKENS:
                reads.add((*_relation_slot(entry.relation), "field"))
    elif isinstance(op, (ops.AddEdgeIndexesOp, ops.RemoveEdgeIndexesOp)):
        for entry in op.edges:
            reads |= {_vertex_slot(entry.source), _vertex_slot(entry.target)}
            if entry.relation is not None:
                reads.add((*_relation_slot(entry.relation), "field"))
    elif isinstance(op, ops.SetFieldSemanticsOp):
        for target in op.targets:
            if isinstance(target, ops.EdgeFieldSemanticsTarget):
                reads |= {_vertex_slot(target.source), _vertex_slot(target.target)}

    # ── edges, addressed by relation name ───────────────────────────────────
    elif isinstance(op, ops.RemoveEdgesOp):
        for selector in op.edges:
            reads |= {_vertex_slot(selector.source), _vertex_slot(selector.target)}
        for relation in op.relations:
            reads |= _relation_endpoints(base, relation, aliases)
    elif isinstance(op, ops.AddEdgePropertiesOp):
        for relation in op.additions:
            reads |= _relation_endpoints(base, relation, aliases)
        reads |= _per_edge_reads(op.per_edge)
    elif isinstance(op, ops.RemoveEdgePropertiesOp):
        for relation in op.removals:
            reads |= _relation_endpoints(base, relation, aliases)
        reads |= _per_edge_reads(op.per_edge)
    elif isinstance(op, ops.RenameEdgePropertiesOp):
        for relation in op.renames:
            reads |= _relation_endpoints(base, relation, aliases)
        reads |= _per_edge_reads(op.per_edge)
    elif isinstance(op, ops.RenameRelationsOp):
        for relation in op.renames:
            reads |= _relation_endpoints(base, relation, aliases)
    elif isinstance(op, ops.MergeEdgesOp):
        for relation in [*op.sources, op.into]:
            reads |= _relation_endpoints(base, relation, aliases)
    elif isinstance(op, (ops.AddInverseEdgesOp, ops.SetNativeInversesOp)):
        for relation in op.relations or []:
            reads |= _relation_endpoints(base, relation, aliases)
    elif isinstance(op, ops.RetractEdgeInversesOp):
        for relation in op.relations:
            reads |= _relation_endpoints(base, relation, aliases)
    elif isinstance(op, ops.DeclareEdgeInversesOp):
        for relation in [*op.inverses, *op.inverses.values(), *op.symmetric]:
            reads |= _relation_endpoints(base, relation, aliases)
    elif isinstance(op, ops.ChangeFieldTypesOp):
        for relation in op.edges:
            reads |= _relation_endpoints(base, relation, aliases)
        reads |= _per_edge_reads(op.per_edge)

    # ── keys ────────────────────────────────────────────────────────────────
    elif isinstance(op, ops.ReplaceIdentityOp):
        for vertex, replacement in op.replacements.items():
            reads |= _key_field_reads(vertex, _identity_target_fields(replacement.to))
    elif isinstance(op, ops.AddSecondaryIdentitiesOp):
        for vertex, entries in op.additions.items():
            for entry in entries:
                reads |= _key_field_reads(vertex, list(entry.fields))
    elif isinstance(op, ops.AddVertexIndexesOp):
        for vertex, indexes in op.indexes.items():
            for index in indexes:
                reads |= {_field_slot(vertex, field) for field in index.fields}

    # ── ingestion ───────────────────────────────────────────────────────────
    elif isinstance(op, (ops.AddResourcesOp, ops.ReplaceResourcesOp)):
        for resource in op.resources:
            reads |= _pipeline_reads(resource)

    return reads


def _pipeline_reads(resource: Any) -> set[Slot]:
    """What a resource's pipeline names: vertices, fixed relations, mapped fields.

    A mapped field is a vertex property a step fills from a named column
    (a vertex step's ``from``, a router's ``vertex_from_map``). A property a
    step fills by its own name is not listed.
    """
    from graflo.architecture.contract.ingestion.steps.ref import iter_edge_steps

    reads = {_vertex_slot(name) for name in resource.collect_vertex_names()}
    for view in iter_edge_steps(list(resource.pipeline)):
        for relation in view.relations_written() or set():
            reads.add(_relation_slot(relation))
    for vertex, field in _mapped_fields(list(resource.pipeline)):
        reads.add(_field_slot(vertex, field))
    return reads


def _mapped_fields(pipeline: list[Any]) -> set[tuple[str, str]]:
    """``(vertex, property)`` for every property a step maps from a named column."""
    from graflo.architecture.contract.ingestion.steps.normalize import (
        normalize_actor_step,
    )

    found: set[tuple[str, str]] = set()
    for raw in pipeline:
        if not isinstance(raw, dict):
            continue
        step = normalize_actor_step(dict(raw))
        kind = step.get("type")
        if kind == "vertex" and isinstance(step.get("from"), dict):
            found |= {(step["vertex"], field) for field in step["from"]}
        elif kind == "vertex_router" and isinstance(step.get("vertex_from_map"), dict):
            for vertex, mapping in step["vertex_from_map"].items():
                if isinstance(mapping, dict):
                    found |= {(vertex, field) for field in mapping}
        elif kind == "descend" and isinstance(step.get("pipeline"), list):
            found |= _mapped_fields(step["pipeline"])
    return found


def _depends_on(read: Slot, written: Slot) -> bool:
    """A read is disturbed by a write at or above it, not by one beneath it."""
    return _covers(written, read)


def ops_independent(
    one: ManifestOp, other: ManifestOp, base: GraphManifest | None = None
) -> bool:
    """Whether *one* and *other* can be applied in either order to one effect.

    Neither writes where the other writes, and neither writes what the other
    reads. Two ops reading the same thing are independent: two edges onto one
    vertex do not get in each other's way.
    """
    one_writes, other_writes = op_slots(one), op_slots(other)
    if any(_covers(a, b) or _covers(b, a) for a in one_writes for b in other_writes):
        return False
    return not any(
        _depends_on(read, written)
        for reader, writes in ((one, other_writes), (other, one_writes))
        for read in op_reads(reader, base)
        for written in writes
    )


# ── merge base ──────────────────────────────────────────────────────────────


def find_merge_base(history: Any, left: str, right: str) -> str | None:
    """The best common ancestor of *left* and *right*, or ``None``.

    "Best" is the common ancestor furthest from the roots, ties broken on
    commit id so the choice is deterministic. Multiple genuinely-incomparable
    bases (a criss-cross history) are picked between with a warning rather than
    handled properly: recursive merge is a known upgrade path and is explicitly
    out of scope here, because real criss-cross histories do not arise until
    people are merging merges routinely.

    Args:
        history: A ``History`` (kept structural to avoid an import cycle).
        left: One commit id.
        right: The other commit id.

    Returns:
        The merge-base commit id, or ``None`` when the two share no ancestor --
        which means they are unrelated lineages, and the operation you want is
        a union (``merge_manifests``), not a three-way merge.
    """
    left_ancestors = history.ancestors(left, include_self=True)
    right_ancestors = history.ancestors(right, include_self=True)
    common = left_ancestors & right_ancestors
    if not common:
        return None

    generation = _generations(history)
    candidates = sorted(common, key=lambda cid: (-generation.get(cid, 0), cid))
    best = candidates[0]

    # A candidate that is an ancestor of the chosen one is subsumed, not rival.
    rivals = [
        cid
        for cid in candidates[1:]
        if generation.get(cid, 0) == generation.get(best, 0)
    ]
    if rivals:
        logger.warning(
            "multiple merge bases for %s and %s (%s); picking %s deterministically. "
            "Recursive merge is not implemented",
            left[:8],
            right[:8],
            ", ".join(cid[:8] for cid in [best, *rivals]),
            best[:8],
        )
    return best


def _generations(history: Any) -> dict[str, int]:
    """Longest path from any root to each commit."""
    generation: dict[str, int] = {}
    for commit in history.topological():
        if not commit.parents:
            generation[commit.id] = 0
        else:
            generation[commit.id] = 1 + max(
                generation.get(parent, 0) for parent in commit.parents
            )
    return generation


# ── conflicts and resolutions ───────────────────────────────────────────────


class MergeConflict(ConfigBaseModel):
    """One slot both sides changed, differently."""

    slot: list[str] = PydanticField(
        ..., description="The contested location, as path segments."
    )
    left_ops: list[RevisionOp] = PydanticField(
        default_factory=list, description="What the left side did here."
    )
    right_ops: list[RevisionOp] = PydanticField(
        default_factory=list, description="What the right side did here."
    )
    base_excerpt: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="The ancestor's state at this slot, for a human deciding.",
    )
    reason: str = PydanticField(
        default="both sides changed this slot",
        description="Why this could not be merged automatically.",
    )

    @property
    def slot_key(self) -> Slot:
        return tuple(self.slot)


class ConflictResolution(ConfigBaseModel):
    """The decision for one contested slot.

    Take-left and take-right are not special cases: they are this model holding
    the corresponding side's op list. A hand-written third answer is expressed
    the same way, which is what lets a recorded resolution replay on a re-merge.
    """

    slot: list[str] = PydanticField(..., description="The slot being resolved.")
    ops: list[RevisionOp] = PydanticField(
        default_factory=list,
        description="Ops to apply for this slot; empty means 'neither side'.",
    )
    rationale: str | None = PydanticField(
        default=None, description="Why, for the next reader."
    )

    @property
    def slot_key(self) -> Slot:
        return tuple(self.slot)


class MergeResult(ConfigBaseModel):
    """What a merge produced, or could not."""

    ops: list[RevisionOp] = PydanticField(
        default_factory=list,
        description="Ops applied to the base to reach the merged manifest.",
    )
    conflicts: list[MergeConflict] = PydanticField(
        default_factory=list, description="Slots needing a decision, slot-sorted."
    )
    warnings: list[str] = PydanticField(default_factory=list)
    merged_hash: str | None = PydanticField(
        default=None, description="Content hash of the merged manifest, if one exists."
    )

    @property
    def clean(self) -> bool:
        """Whether the merge completed with no decisions left to make."""
        return not self.conflicts


# ── the merge ───────────────────────────────────────────────────────────────


def _canonical_ops(ops: list[Any]) -> str:
    return json.dumps(ops_to_dicts(list(ops)), sort_keys=True, separators=(",", ":"))


def _covers(outer: Slot, inner: Slot) -> bool:
    """Whether *outer* is *inner* or an ancestor of it.

    Slots form a tree -- ``("vertex", "person")`` contains
    ``("vertex", "person", "field", "age")`` and
    ``("vertex", "person", "identity")`` -- and containment is what makes a
    rename collide with an edit to the renamed thing. Comparing slots for
    *equality* alone would let one side rename ``person`` while the other adds
    a field to ``person``, merge both cleanly, and produce a change set whose
    second half addresses a vertex the first half removed.
    """
    return len(outer) <= len(inner) and inner[: len(outer)] == outer


def _touches(slots: set[Slot], slot: Slot) -> bool:
    """Whether any of *slots* contains, or is contained by, *slot*."""
    return any(
        _covers(candidate, slot) or _covers(slot, candidate) for candidate in slots
    )


def _contact_points(left: set[Slot], right: set[Slot]) -> set[Slot]:
    """Where two slot sets meet, reported at the most general slot involved.

    Reporting at the shorter slot is what makes the conflict legible: a rename
    of ``person`` against three separate field edits is one conflict about
    ``vertex/person``, not three about fields that merely happen to live there.
    """
    points: set[Slot] = set()
    for one in left:
        for other in right:
            if _covers(one, other):
                points.add(one)
            elif _covers(other, one):
                points.add(other)
    return points


def _ops_touching(ops: list[ManifestOp], slot: Slot) -> list[ManifestOp]:
    """The ops that reach *slot*, in their original order."""
    return [op for op in ops if _touches(op_slots(op), slot)]


def _dependencies(
    readers: list[ManifestOp], writers: list[ManifestOp], base: GraphManifest
) -> dict[str, set[Slot]]:
    """For each of *readers*, the slots *writers* change out from under it.

    Keyed by the reader's canonical form. An op both sides made is agreement:
    it is on the reader's own side too, where the differ already ordered it, so
    it is neither a reader nor a writer here.
    """
    shared = {_canonical_ops([op]) for op in readers} & {
        _canonical_ops([op]) for op in writers
    }
    found: dict[str, set[Slot]] = {}
    aliases = relation_aliases(readers)
    for reader in readers:
        key = _canonical_ops([reader])
        reads = op_reads(reader, base, aliases)
        if key in shared or not reads:
            continue
        for writer in writers:
            if _canonical_ops([writer]) in shared:
                continue
            for written in op_slots(writer):
                if any(_depends_on(read, written) for read in reads):
                    found.setdefault(key, set()).add(written)
    return found


def _base_excerpt(base: GraphManifest, slot: Slot) -> dict[str, Any]:
    """The ancestor's state at *slot*, best effort, for a human deciding."""
    if not slot or base.graph_schema is None:
        return {}
    if slot[0] == "vertex":
        target = slot[1]
        for vertex in base.graph_schema.core_schema.vertex_config.vertices:
            if "/".join(canonical_key(vertex.name)) == target:
                return vertex.to_minimal_canonical_dict()
    if slot[0] == "relation":
        edges = [
            edge
            for edge in base.graph_schema.core_schema.edge_config.edges
            if edge.relation is not None
            and _relation_slot(edge.relation) == slot[:2]
            and (
                len(slot) < 5
                or slot[2] != "edge"
                or _edge_slot(edge.source, edge.target, edge.relation) == slot[:5]
            )
        ]
        if edges:
            return {"edges": [edge.to_minimal_canonical_dict() for edge in edges]}
    if slot[0] == "resource" and base.ingestion_model is not None:
        for resource in base.ingestion_model.resources:
            if resource.name == slot[1]:
                return resource.to_minimal_canonical_dict()
    return {}


def merge_three_way(
    base: GraphManifest,
    left: GraphManifest,
    right: GraphManifest,
    *,
    resolutions: list[ConflictResolution] | None = None,
    hints: RenameHints | None = None,
    left_hints: RenameHints | None = None,
    right_hints: RenameHints | None = None,
) -> tuple[GraphManifest | None, MergeResult]:
    """Reconcile *left* and *right*, both descended from *base*.

    Args:
        base: The common ancestor.
        left: One descendant. Its ops are applied first.
        right: The other descendant.
        resolutions: Decisions for contested slots. A slot with a resolution is
            no longer a conflict; its ops replace both sides' at that slot.
        hints: Rename hints for the two diffs -- renames are never *inferred*
            (a drop plus an add is not a rename), so a rename on either side
            needs a hint to be seen as one.
        left_hints: Hints for the left diff alone, in place of *hints*. A
            hint is emitted as a rename, so a rename only one side made belongs
            in that side's hints (see :meth:`RenameHints.from_ops`).
        right_hints: Hints for the right diff alone, in place of *hints*.

    Returns:
        ``(merged_manifest_or_None, result)``. The manifest is ``None`` exactly
        when unresolved conflicts remain.

    Raises:
        MergeError: A side changed something no operation expresses -- it
            dropped the ingestion block, say -- so the merged manifest would
            silently lack it. Also when the
            merged change set does not apply to *base*.
    """
    from .apply import apply_evolution

    resolved: dict[Slot, ConflictResolution] = {
        resolution.slot_key: resolution for resolution in (resolutions or [])
    }

    left_ops, left_warnings = diff_manifests_verified(
        base, left, hints=left_hints or hints
    )
    right_ops, right_warnings = diff_manifests_verified(
        base, right, hints=right_hints or hints
    )
    warnings = [f"left: {w}" for w in left_warnings]
    warnings += [f"right: {w}" for w in right_warnings]
    if warnings:
        # The merge is assembled from each side's ops. A change the ops do not
        # carry is a change the merged manifest will not have, and a result
        # that is clean and incomplete is the one outcome a merge may not have.
        raise MergeError(
            "a side changed something no operation expresses, so merging would "
            "drop it: " + "; ".join(warnings)
        )

    left_ops = [piece for op in left_ops for piece in split_by_slot(op)]
    right_ops = [piece for op in right_ops for piece in split_by_slot(op)]

    left_slots = {slot for op in left_ops for slot in op_slots(op)}
    right_slots = {slot for op in right_ops for slot in op_slots(op)}

    # A slot is contested when both sides reach it and disagree about how.
    contested: set[Slot] = set()
    for slot in sorted(_contact_points(left_slots, right_slots)):
        if _canonical_ops(_ops_touching(left_ops, slot)) != _canonical_ops(
            _ops_touching(right_ops, slot)
        ):
            contested.add(slot)

    # A slot is contested, too, when one side changes what the other side's
    # change depends on: an edge added onto a vertex the other side removed.
    # The two ops write different slots, so nothing above sees them meet.
    depends: dict[str, set[Slot]] = _dependencies(left_ops, right_ops, base)
    for key, slots in _dependencies(right_ops, left_ops, base).items():
        depends.setdefault(key, set()).update(slots)
    depended_on = {slot for slots in depends.values() for slot in slots}
    written_by_both = set(contested)
    contested |= depended_on

    def reaches(op: ManifestOp, slot: Slot) -> bool:
        """Whether *op* writes at *slot* or depends on what is written there."""
        return _touches(op_slots(op), slot) or slot in depends.get(
            _canonical_ops([op]), ()
        )

    def reason_for(slot: Slot) -> str:
        if slot == ("manifest",):
            return "a whole-manifest op cannot be merged with another change"
        if slot in written_by_both:
            return "both sides changed this slot differently"
        return "one side changed this slot and the other side's change depends on it"

    unresolved = sorted(contested - set(resolved))
    conflicts = [
        MergeConflict(
            slot=list(slot),
            left_ops=ops_from_dicts(
                ops_to_dicts([op for op in left_ops if reaches(op, slot)])
            ),
            right_ops=ops_from_dicts(
                ops_to_dicts([op for op in right_ops if reaches(op, slot)])
            ),
            base_excerpt=_base_excerpt(base, slot),
            reason=reason_for(slot),
        )
        for slot in unresolved
    ]

    # An op is held back if *any* slot it reaches is contested: applying half an
    # op is not a merge, and neither is applying an op whose ground moved.
    def blocked(op: ManifestOp) -> bool:
        return any(reaches(op, slot) for slot in contested)

    # Assemble in diff order, with each resolution taking the *place* of the
    # ops it replaces rather than being appended at the end.
    #
    # Order is a precondition, not a presentation detail: `diff_manifests`
    # emits renames before additions before identity changes before removals,
    # so that each op's preconditions hold when it runs. Appending resolutions
    # last inverts that -- a re-key resolution would land *after* the demoted
    # key was added as a secondary identity, and the apply fails on a duplicate
    # that only exists because of the reordering.
    merged_ops: list[ManifestOp] = []
    applied_canonical: set[str] = set()
    placed_slots: set[Slot] = set()

    def emit(op: ManifestOp) -> None:
        # Both sides making the identical change is agreement, not duplication.
        canonical = _canonical_ops([op])
        if canonical in applied_canonical:
            return
        applied_canonical.add(canonical)
        merged_ops.append(op)

    def place_resolutions_for(op: ManifestOp) -> None:
        """Emit the decisions for whichever contested slots *op* reaches."""
        for contested_slot in sorted(contested):
            if contested_slot in placed_slots:
                continue
            if not reaches(op, contested_slot):
                continue
            placed_slots.add(contested_slot)
            resolution = resolved.get(contested_slot)
            if resolution is not None:
                for resolution_op in resolution.ops:
                    emit(resolution_op)

    for op in [*left_ops, *right_ops]:
        if blocked(op):
            place_resolutions_for(op)
        else:
            emit(op)

    # A decision for a slot no op reached (both sides' ops were deduplicated
    # away, say) still belongs in the change set.
    for slot in sorted(contested - placed_slots):
        resolution = resolved.get(slot)
        if resolution is not None:
            for resolution_op in resolution.ops:
                emit(resolution_op)
    if conflicts:
        return None, MergeResult(ops=[], conflicts=conflicts, warnings=warnings)

    if not merged_ops:
        # Both sides are the base, or their changes cancelled out.
        return base, MergeResult(
            ops=[], conflicts=[], warnings=warnings, merged_hash=manifest_hash(base)
        )

    try:
        merged = apply_evolution(
            base, merged_ops, bump_version=False, finish_init=False
        )
    except Exception as exc:
        raise MergeError(
            "the merged change set does not apply to the base: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    return merged, MergeResult(
        ops=ops_from_dicts(ops_to_dicts(merged_ops)),
        conflicts=[],
        warnings=warnings,
        merged_hash=manifest_hash(merged),
    )


# ── tracked merges ──────────────────────────────────────────────────────────


class MergeRecipe(ConfigBaseModel):
    """How a merge was performed, recorded so it can be performed again.

    This is the ``rerere`` analogue: when the left side advances and the same
    merge is run again, the recorded resolutions are re-applied to any slot that
    conflicts *again*, and only genuinely new conflicts reach a human. That is
    what makes a tracked merge cheap enough to keep re-running, which is what
    makes an overlay maintainable rather than a one-time fork.
    """

    kind: str = PydanticField(
        default="merge3", description="merge3 (common ancestor) or merge (unrelated)."
    )
    left: str = PydanticField(..., description="Content hash of the left state.")
    right: str = PydanticField(..., description="Content hash of the right state.")
    base: str | None = PydanticField(
        default=None, description="Content hash of the merge base; merge3 only."
    )
    resolutions: list[ConflictResolution] = PydanticField(
        default_factory=list, description="Slot-keyed decisions, replayable."
    )
    equivalences: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Declared alignment for a merge; empty for merge3.",
    )
    name_conflict: str | None = PydanticField(
        default=None, description="Merge's name-conflict policy, when applicable."
    )

    def content_hash(self) -> str:
        """Content address of this recipe.

        Resolutions are hashed in slot order, not in the order a human happened
        to supply them, so the same decisions always address the same recipe.
        """
        payload = {
            "kind": self.kind,
            "left": self.left,
            "right": self.right,
            "base": self.base,
            "equivalences": self.equivalences,
            "name_conflict": self.name_conflict,
            "resolutions": sorted(
                (
                    {
                        "slot": list(resolution.slot),
                        "ops": ops_to_dicts(list(resolution.ops)),
                    }
                    for resolution in self.resolutions
                ),
                key=lambda entry: json.dumps(entry["slot"]),
            ),
        }
        return suthing.stable_hash(payload)


def build_merge_recipe(
    left: GraphManifest,
    right: GraphManifest,
    op: ops.MergeManifestsOp,
) -> MergeRecipe:
    """Record how a merge was *declared*, addressed by content.

    The merge counterpart to :func:`build_recipe`. Two things differ, and both
    follow from merge joining unrelated lineages rather than reconciling
    related ones: there is no merge base, so ``base`` is ``None``; and there are
    no conflicts to resolve, because merge refuses rather than resolving, so
    ``resolutions`` stays empty.

    What takes their place is the declaration itself. The whole op is recorded
    -- equivalences and their identities, canonical maps, resource renames and
    the name-conflict policy -- because all of it is "how these two were
    joined", and a re-merge that had only the equivalences would reconstruct a
    different manifest.

    When the union names a demoted key or an untagged local key by a side's
    origin, the resolved ``origins`` are recorded with the op, so a re-merge
    names them alike after a parent's schema is renamed. Otherwise the op is
    recorded as given.
    """
    from .naming_graph import build_naming, merge_origins

    resolution = build_naming(
        op, left=left.model_copy(deep=True), right=right.model_copy(deep=True)
    ).resolution
    names_by_origin = any(
        space == resolution.origins[side]
        for (side, _member), space in resolution.key_spaces.items()
    ) or any(resolution.origin_tagged.values())
    recorded = (
        op.model_copy(update={"origins": merge_origins(op, left, right)})
        if names_by_origin
        else op
    )
    return MergeRecipe(
        kind="merge",
        left=manifest_hash(left),
        right=manifest_hash(right),
        base=None,
        resolutions=[],
        equivalences=recorded.to_dict(),
        name_conflict=op.name_conflict,
    )


def build_recipe(
    base: GraphManifest | None,
    left: GraphManifest,
    right: GraphManifest,
    *,
    resolutions: list[ConflictResolution] | None = None,
    kind: str = "merge3",
) -> MergeRecipe:
    """Record how this merge was resolved, addressed by content."""
    return MergeRecipe(
        kind=kind,
        left=manifest_hash(left),
        right=manifest_hash(right),
        base=manifest_hash(base) if base is not None else None,
        resolutions=list(resolutions or []),
    )


def re_merge(
    recipe: MergeRecipe,
    base: GraphManifest,
    left: GraphManifest,
    right: GraphManifest,
    *,
    hints: RenameHints | None = None,
    left_hints: RenameHints | None = None,
    right_hints: RenameHints | None = None,
) -> tuple[GraphManifest | None, MergeResult]:
    """Re-run a recorded merge with its resolutions pre-applied.

    The point of a *tracked* merge: when the left side advances, this replays
    the decisions already made and surfaces only conflicts that are genuinely
    new. Recorded resolutions for slots that no longer conflict are simply not
    needed and are reported as such, rather than being force-applied -- a stale
    decision reapplied to a slot nobody contested is how a re-merge quietly
    reverts someone's work.

    Args:
        recipe: The recorded merge.
        base: The (possibly new) merge base.
        left: The (possibly advanced) left state.
        right: The right state.
        hints: Rename hints for the underlying diffs.
        left_hints: Hints for the left diff alone, as in :func:`merge_three_way`.
        right_hints: Hints for the right diff alone.

    Returns:
        ``(merged_or_None, result)``, as :func:`merge_three_way`.
    """

    # What *would* conflict this time, before any recorded decision is applied.
    # Comparing against the conflicts that survive the replay instead would
    # report every resolution that did its job as "not needed" -- precisely
    # backwards, and the reading a maintainer would act on by deleting it.
    def run(resolutions: list[ConflictResolution] | None):
        return merge_three_way(
            base,
            left,
            right,
            resolutions=resolutions,
            hints=hints,
            left_hints=left_hints,
            right_hints=right_hints,
        )

    _unresolved, dry = run(None)
    contested = {conflict.slot_key for conflict in dry.conflicts}

    merged, result = run(recipe.resolutions)

    recorded = {resolution.slot_key for resolution in recipe.resolutions}
    replayed = sorted(recorded & contested)
    if replayed:
        result.warnings.append(
            f"{len(replayed)} recorded resolution(s) replayed: "
            + ", ".join(describe_slot(slot) for slot in replayed)
        )
    unused = sorted(recorded - contested)
    if unused:
        result.warnings.append(
            f"{len(unused)} recorded resolution(s) were not needed this time: "
            + ", ".join(describe_slot(slot) for slot in unused)
        )
    genuinely_new = sorted(contested - recorded)
    if genuinely_new:
        result.warnings.append(
            f"{len(genuinely_new)} conflict(s) are new since the recorded merge: "
            + ", ".join(describe_slot(slot) for slot in genuinely_new)
        )
    return merged, result


def take_left(
    conflict: MergeConflict, *, rationale: str | None = None
) -> ConflictResolution:
    """Resolve *conflict* by keeping the left side's ops."""
    return ConflictResolution(
        slot=list(conflict.slot),
        ops=ops_from_dicts(ops_to_dicts(list(conflict.left_ops))),
        rationale=rationale or "took left",
    )


def take_right(
    conflict: MergeConflict, *, rationale: str | None = None
) -> ConflictResolution:
    """Resolve *conflict* by keeping the right side's ops."""
    return ConflictResolution(
        slot=list(conflict.slot),
        ops=ops_from_dicts(ops_to_dicts(list(conflict.right_ops))),
        rationale=rationale or "took right",
    )


#: Ops that move a thing to a new name: whatever was applied to the old name
#: before them travels with it.
_RENAMING_OPS: tuple[type, ...] = (
    ops.RenameVerticesOp,
    ops.RenameVertexPropertiesOp,
    ops.RenameRelationsOp,
    ops.RenameEdgePropertiesOp,
    ops.RenameResourcesOp,
    ops.CanonicalizeOp,
)


def _renames(side_ops: list[Any]) -> bool:
    return any(isinstance(op, _RENAMING_OPS) for op in side_ops)


def keep_both(
    base: GraphManifest, conflict: MergeConflict, *, rationale: str | None = None
) -> ConflictResolution | None:
    """Resolve *conflict* by keeping both sides' ops, when one side renames.

    The renaming side's ops go last, so its rename carries the other side's
    edits to the new name and no op needs rewriting.

    Returns ``None`` unless exactly one side renames -- without a rename, both
    op lists in sequence mean the later one wins, which is not "both" -- or when
    the combined ops do not apply to *base*. :func:`merge_three_way` applies a resolution to the base,
    preceded only by ops that neither touch nor read its slot, so applying it to
    *base* alone is the same check.
    """
    from .apply import apply_evolution

    left_ops, right_ops = list(conflict.left_ops), list(conflict.right_ops)
    left_renames, right_renames = _renames(left_ops), _renames(right_ops)
    if left_renames == right_renames:
        return None
    ordered = [*right_ops, *left_ops] if left_renames else [*left_ops, *right_ops]
    resolution = ConflictResolution(
        slot=list(conflict.slot),
        ops=ops_from_dicts(ops_to_dicts(ordered)),
        rationale=rationale or "kept both sides",
    )
    try:
        apply_evolution(
            base, list(resolution.ops), bump_version=False, finish_init=False
        )
    except Exception:
        return None
    return resolution


def describe_slot(slot: Slot) -> str:
    """A slot as a human reads it: ``vertex/person/field/age``."""
    return "/".join(slot)


__all__ = [
    "ConflictResolution",
    "MergeConflict",
    "MergeError",
    "MergeRecipe",
    "MergeResult",
    "Slot",
    "build_merge_recipe",
    "build_recipe",
    "describe_slot",
    "find_merge_base",
    "keep_both",
    "merge_three_way",
    "op_slots",
    "re_merge",
    "take_left",
    "take_right",
]


# Imported for its side effect on the public surface only.
_ = op_to_dict
