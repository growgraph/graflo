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
are a :class:`MergeConflict`, reported rather than guessed at. What each op
writes and reads -- its footprint -- is defined in :mod:`.footprint`; this
module merges on it.

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
  the whole op is held back. Applying half an op is not a merge. An op held
  by several conflicts is applied only when every one of their decisions
  takes it.
* An op **reads** as well as writes. An edge added onto ``company`` writes the
  edge and depends on ``company``; the other side removing ``company`` writes a
  different slot and still cannot be merged with it (:func:`op_reads`). A read
  is disturbed by a write at or above it, never by one beneath: the edge does
  not care which fields ``company`` carries.
* An op addressed to **every edge of a relation** reads the relation's edge
  set, which any edge written directly under the relation disturbs: adding a
  property to every ``knows`` edge conflicts with the other side adding a
  ``knows`` edge, since the op would reach that edge in one order only.

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
from typing import Any

import suthing
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.naming import canonical_key

from . import ops
from .autogenerate import RenameHints, diff_manifests_verified
from .codec import RevisionOp, op_to_dict, ops_from_dicts, ops_to_dicts
from .footprint import (
    WHOLE_MANIFEST,
    Slot,
    covers,
    depends_on,
    describe_slot,
    edge_slot,
    op_reads,
    op_slots,
    relation_aliases,
    relation_slot,
)
from .hashing import manifest_hash
from .ops import ManifestOp

logger = logging.getLogger(__name__)


class MergeError(RuntimeError):
    """A merge cannot proceed: unmergeable inputs, or an invalid resolution."""


# ── one op per slot ─────────────────────────────────────────────────────────

#: How an op divides into one op per slot: each payload field to split, and how.
#: ``items`` splits a list per element, ``keys`` a mapping per key, ``entries`` a
#: mapping per key *and* per element (or inner key) under it, ``relation`` a
#: list of edges per relation. An op kind absent here is never split: an inverse
#: table, a whole block. A resource is one piece, its pipeline never split.
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
    # Every edge of a relation agrees on `directed`: one piece per relation.
    ops.SetEdgeDirectedOp: (("edges", "relation"),),
    ops.SetEdgeSemanticsOp: (("edges", "items"),),
    ops.SetEdgeDescriptionsOp: (("edges", "items"),),
    ops.SetEdgeSubpropertyOfOp: (("edges", "items"),),
    ops.RenameRelationsOp: (("renames", "keys"),),
    ops.AddEdgePropertiesOp: (("additions", "keys"), ("per_edge", "items")),
    ops.RemoveEdgePropertiesOp: (("removals", "keys"), ("per_edge", "items")),
    ops.RenameEdgePropertiesOp: (("renames", "keys"), ("per_edge", "items")),
    ops.AddResourcesOp: (("resources", "items"),),
    ops.ReplaceResourcesOp: (("resources", "items"),),
    ops.RemoveResourcesOp: (("names", "items"),),
    ops.RenameResourcesOp: (("renames", "keys"),),
    ops.AddResourceTransformsOp: (("additions", "keys"),),
    ops.EnsureExtractedFieldsOp: (("additions", "keys"),),
    ops.SetInverseEmissionOp: (("steps", "keys"),),
}


def _rename_chains(op: ManifestOp) -> bool:
    """Whether a rename map renames onto a name it also renames away.

    ``{a: b, b: a}`` is a swap only as one op: split, each half collides with
    the name the other still holds.
    """
    if isinstance(op, (ops.RenameVerticesOp, ops.RenameRelationsOp)):
        maps = [op.renames]
    elif isinstance(op, ops.RenameResourcesOp):
        # Resource names are compared as written, not canonically.
        return bool(set(op.renames.values()) & set(op.renames))
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
        elif how == "relation":
            groups: dict[Slot | None, list[Any]] = {}
            for item in value:
                relation = item.get("relation")
                key = relation_slot(relation) if relation is not None else None
                groups.setdefault(key, []).append(item)
            pieces += [{name: items} for items in groups.values()]
        else:
            for key, inner in value.items():
                if isinstance(inner, dict):
                    pieces += [{name: {key: {k: v}}} for k, v in inner.items()]
                else:
                    pieces += [{name: {key: [entry]}} for entry in inner]
    if len(pieces) <= 1:
        return [op]
    return [op_from_dict({**common, **piece}) for piece in pieces]


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
    cascaded: list[RevisionOp] = PydanticField(
        default_factory=list,
        description=(
            "The ops of either side held here only through their cascade: a "
            "removal or rename that rewrites this slot without naming it."
        ),
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


def _touches(slots: set[Slot], slot: Slot) -> bool:
    """Whether any of *slots* contains, or is contained by, *slot*."""
    return any(
        covers(candidate, slot) or covers(slot, candidate) for candidate in slots
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
            if covers(one, other):
                points.add(one)
            elif covers(other, one):
                points.add(other)
    return points


def _ops_touching(
    ops: list[ManifestOp], slot: Slot, base: GraphManifest
) -> list[ManifestOp]:
    """The ops that reach *slot*, in their original order."""
    return [op for op in ops if _touches(op_slots(op, base), slot)]


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
            for written in op_slots(writer, base):
                if any(depends_on(read, written) for read in reads):
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
            and relation_slot(edge.relation) == slot[:2]
            and (
                len(slot) < 5
                or slot[2] != "edge"
                or edge_slot(edge.source, edge.target, edge.relation) == slot[:5]
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

    left_slots = {slot for op in left_ops for slot in op_slots(op, base)}
    right_slots = {slot for op in right_ops for slot in op_slots(op, base)}

    # A slot is contested when both sides reach it and disagree about how.
    contested: set[Slot] = set()
    for slot in sorted(_contact_points(left_slots, right_slots)):
        if _canonical_ops(_ops_touching(left_ops, slot, base)) != _canonical_ops(
            _ops_touching(right_ops, slot, base)
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
        return _touches(op_slots(op, base), slot) or slot in depends.get(
            _canonical_ops([op]), ()
        )

    def cascaded(op: ManifestOp, slot: Slot) -> bool:
        """Whether *op* reaches *slot* only through what its cascade rewrites."""
        return (
            _touches(op_slots(op, base), slot)
            and not _touches(op_slots(op), slot)
            and slot not in depends.get(_canonical_ops([op]), ())
        )

    def reason_for(slot: Slot) -> str:
        if slot == WHOLE_MANIFEST:
            return "a whole-manifest op cannot be merged with another change"
        for side in (left_ops, right_ops):
            held = [op for op in side if reaches(op, slot)]
            if held and all(cascaded(op, slot) for op in held):
                return (
                    "one side's change reaches this slot only through what it "
                    "removes or renames"
                )
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
            cascaded=ops_from_dicts(
                ops_to_dicts(
                    [op for op in [*left_ops, *right_ops] if cascaded(op, slot)]
                )
            ),
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

    # A side's op held by several conflicts follows all their decisions, not
    # any one of them: it is applied only when every one of them takes it.
    held: dict[str, set[Slot]] = {}
    for op in [*left_ops, *right_ops]:
        reached = {slot for slot in contested if reaches(op, slot)}
        if reached:
            held.setdefault(_canonical_ops([op]), set()).update(reached)
    taken: dict[Slot, set[str]] = {
        slot: {
            _canonical_ops([piece])
            for resolution_op in resolution.ops
            for piece in split_by_slot(resolution_op)
        }
        for slot, resolution in resolved.items()
    }

    def decided(resolution_op: ManifestOp) -> list[ManifestOp]:
        """What of *resolution_op* the decisions agree to apply.

        A hand-written op applies as given; a side's op, or a piece of one,
        only when every conflict holding it takes it.
        """
        pieces = split_by_slot(resolution_op)
        keys = [_canonical_ops([piece]) for piece in pieces]
        if not all(key in held for key in keys):
            return [resolution_op]
        return [
            piece
            for piece, key in zip(pieces, keys)
            if all(key in taken.get(slot, ()) for slot in held[key])
        ]

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
                    for piece in decided(resolution_op):
                        emit(piece)

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
                for piece in decided(resolution_op):
                    emit(piece)
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

    When the union names a demoted key by a side's origin, the resolved ``origins`` are recorded with the op, so a re-merge
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
    )
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


__all__ = [
    "ConflictResolution",
    "MergeConflict",
    "MergeError",
    "MergeRecipe",
    "MergeResult",
    "build_merge_recipe",
    "build_recipe",
    "find_merge_base",
    "keep_both",
    "merge_three_way",
    "re_merge",
    "take_left",
    "take_right",
]


# Imported for its side effect on the public surface only.
_ = op_to_dict
