"""Three-way merge over manifest change sets, and tracked re-merges.

Merging two world models is not diffing them. Both sides descend from a common
ancestor, so the question is never "what is different" but "what did each side
*change*, and do those changes collide". That is the three-way shape: diff
base→left, diff base→right, and reconcile.

Slots
-----

Reconciliation happens per **slot** -- the addressable location an op touches,
such as ``("vertex", "person", "field", "age")``. Two sides that touch disjoint
slots merge automatically. Two sides that make the *same* change to one slot
merge to that change, once. Two sides that make different changes to one slot
are a :class:`MergeConflict`, reported rather than guessed at.

Three things make the slot the right unit:

* An **order-significant sequence is one slot**. A resource pipeline is an
  ordered program, and half-merging two edits to a program produces something
  neither author wrote. It conflicts as a unit or it merges as a unit.
* A **rename occupies both names**. Renaming ``person`` → ``customer`` on one
  side while the other side adds a field to ``person`` is a genuine collision,
  and it is invisible unless the rename is understood to touch the old slot too.
* An op touching several slots is **atomic**: if any one of its slots is
  contested, the whole op is held back. Applying half an op is not a merge.

Merge is not compose
--------------------

Merge reconciles two descendants of a **common ancestor**: names are expected to
agree because both sides inherited them, so disagreement is a conflict. Compose
joins **unrelated lineages** by declared equivalence: names are expected to
disagree, and the declaration is what reconciles them. Both produce multi-parent
commits; they are not the same operation and must not be conflated.

Determinism is a contract
-------------------------

The same inputs produce the same merged manifest, the same conflicts in the same
order, and -- through canonical hashing -- the same content hash. Auto-merged
ops are applied left-side-first in their diff order, then right-side. Nothing
here consults a set iteration order or a dict insertion order.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.naming import canonical_key

from . import ops
from .autogenerate import RenameHints, diff_manifests
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

    Keying on ``canonical_key`` rather than the raw string is the fold of
    ``CORE-MERGE-001`` into merge: without it, one side's ``order_line`` and the
    other's ``OrderLine`` occupy different slots, merge cleanly, and produce a
    schema holding both as unrelated types with the data split between them and
    nothing raising. Detecting that is exactly what a conflict is for.
    """
    return ("vertex", "/".join(canonical_key(name)) or name.lower())


def _edge_slot(source: str, target: str, relation: str | None) -> Slot:
    return (
        "edge",
        "/".join(canonical_key(source)) or source.lower(),
        "/".join(canonical_key(target)) or target.lower(),
        (relation or "").lower(),
    )


def _field_slot(vertex: str, field: str) -> Slot:
    return (*_vertex_slot(vertex), "field", "/".join(canonical_key(field)) or field)


def _relation_slot(relation: str) -> Slot:
    """A relation's slot, canonicalized like every other name."""
    return ("relation", "/".join(canonical_key(relation)) or relation.lower())


def _resource_slot(name: str) -> Slot:
    # A resource's pipeline is an ordered program: one slot for the whole
    # resource, never one per step.
    return ("resource", name)


def _edge_key_of(entry: Any) -> tuple[str, str, str | None]:
    """Endpoints of an edge-ish payload, whether model or mapping."""
    if isinstance(entry, dict):
        return (
            str(entry.get("source", "")),
            str(entry.get("target", "")),
            entry.get("relation"),
        )
    return (
        str(getattr(entry, "source", "")),
        str(getattr(entry, "target", "")),
        getattr(entry, "relation", None),
    )


def _name_of(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name", ""))
    return str(getattr(entry, "name", ""))


def op_slots(op: ManifestOp) -> set[Slot]:
    """Every addressable location *op* touches.

    Dispatch is on the op *class*, not on its ``op`` string literal. Two
    reasons: a type checker can narrow it, so a field read against the wrong op
    model is caught at check time rather than at merge time; and a literal
    renamed in ``ops.py`` cannot silently fall through to the catch-all here.

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
        slots |= {_vertex_slot(_name_of(v)) for v in op.vertices}
    elif isinstance(op, ops.RemoveVerticesOp):
        slots |= {_vertex_slot(name) for name in op.names}
    elif isinstance(op, ops.RenameVerticesOp):
        # Both names: a rename collides with any edit to either side of it.
        for old, new in op.vertices.items():
            slots |= {_vertex_slot(old), _vertex_slot(new)}
    elif isinstance(op, ops.MergeVerticesOp):
        slots |= {_vertex_slot(name) for name in op.sources}
        slots.add(_vertex_slot(op.into))

    # ── vertex properties ───────────────────────────────────────────────────
    elif isinstance(op, ops.AddVertexPropertiesOp):
        for vertex, fields in op.additions.items():
            slots |= {_field_slot(vertex, field) for field in fields}
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
        slots |= {(*_vertex_slot(vertex), "identity") for vertex in op.vertices}
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
        slots |= {_edge_slot(*_edge_key_of(entry)) for entry in op.edges}
    elif isinstance(op, ops.SetEdgeDirectedOp):
        slots |= {(*_edge_slot(*_edge_key_of(entry)), "directed") for entry in op.edges}

    # ── edges, addressed by relation name ───────────────────────────────────
    elif isinstance(op, ops.RemoveEdgesOp):
        slots |= {_relation_slot(relation) for relation in op.relations}
    elif isinstance(op, ops.RenameRelationsOp):
        # `{old: new}` -- both names are occupied.
        for old, new in op.relations.items():
            slots |= {_relation_slot(old), _relation_slot(new)}
    elif isinstance(op, ops.AddInverseEdgesOp):
        for relation, inverse in op.relations.items():
            slots |= {_relation_slot(relation), _relation_slot(inverse)}
    elif isinstance(op, ops.MergeEdgesOp):
        slots |= {_relation_slot(relation) for relation in op.sources}
        slots.add(_relation_slot(op.into))

    # ── edge properties ─────────────────────────────────────────────────────
    elif isinstance(op, ops.AddEdgePropertiesOp):
        slots |= {(*_relation_slot(rel), "field") for rel in op.additions}
    elif isinstance(op, ops.RemoveEdgePropertiesOp):
        slots |= {(*_relation_slot(rel), "field") for rel in op.removals}
    elif isinstance(op, ops.RenameEdgePropertiesOp):
        slots |= {(*_relation_slot(rel), "field") for rel in op.renames}

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

    # ── ingestion ───────────────────────────────────────────────────────────
    elif isinstance(op, ops.AddResourceTransformsOp):
        # A pipeline is one slot per resource: an ordered program cannot be
        # half-merged, so it conflicts as a unit or merges as a unit.
        slots |= {_resource_slot(name) for name in op.additions}
    elif isinstance(op, ops.RenameResourcesOp):
        for old, new in op.resources.items():
            slots |= {_resource_slot(old), _resource_slot(new)}

    # ── whole-manifest ops ──────────────────────────────────────────────────
    elif isinstance(
        op, (ops.ProjectManifestOp, ops.SanitizeOp, ops.ComposeManifestsOp)
    ):
        # These rewrite everything, so they conflict with any other change.
        # That is the honest answer: there is no way to merge "keep only these
        # vertices" with an unrelated edit and be sure of the result.
        slots.add(("manifest",))

    if not slots:
        logger.warning(
            "op '%s' has no slot mapping; treating it as touching the whole "
            "manifest, so it will conflict with any other change",
            op.op,
        )
        slots.add(("manifest",))
    return slots


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
        compose, not merge.
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


def _base_excerpt(base: GraphManifest, slot: Slot) -> dict[str, Any]:
    """The ancestor's state at *slot*, best effort, for a human deciding."""
    if not slot or base.graph_schema is None:
        return {}
    if slot[0] == "vertex":
        target = slot[1]
        for vertex in base.graph_schema.core_schema.vertex_config.vertices:
            if "/".join(canonical_key(vertex.name)) == target:
                return vertex.to_minimal_canonical_dict()
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

    Returns:
        ``(merged_manifest_or_None, result)``. The manifest is ``None`` exactly
        when unresolved conflicts remain.
    """
    from .apply import apply_evolution

    resolved: dict[Slot, ConflictResolution] = {
        resolution.slot_key: resolution for resolution in (resolutions or [])
    }

    left_ops, left_warnings = diff_manifests(base, left, hints=hints)
    right_ops, right_warnings = diff_manifests(base, right, hints=hints)
    warnings = [f"left: {w}" for w in left_warnings]
    warnings += [f"right: {w}" for w in right_warnings]

    left_slots = {slot for op in left_ops for slot in op_slots(op)}
    right_slots = {slot for op in right_ops for slot in op_slots(op)}

    # A slot is contested when both sides reach it and disagree about how.
    contested: set[Slot] = set()
    for slot in sorted(_contact_points(left_slots, right_slots)):
        if _canonical_ops(_ops_touching(left_ops, slot)) != _canonical_ops(
            _ops_touching(right_ops, slot)
        ):
            contested.add(slot)

    unresolved = sorted(contested - set(resolved))
    conflicts = [
        MergeConflict(
            slot=list(slot),
            left_ops=ops_from_dicts(ops_to_dicts(_ops_touching(left_ops, slot))),
            right_ops=ops_from_dicts(ops_to_dicts(_ops_touching(right_ops, slot))),
            base_excerpt=_base_excerpt(base, slot),
            reason=(
                "both sides changed this slot differently"
                if slot != ("manifest",)
                else "a whole-manifest op cannot be merged with another change"
            ),
        )
        for slot in unresolved
    ]

    # An op is held back if *any* slot it touches is contested: applying half an
    # op is not a merge.
    def blocked(op: ManifestOp) -> bool:
        slots = op_slots(op)
        return any(_touches(slots, slot) for slot in contested)

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
        slots = op_slots(op)
        for contested_slot in sorted(contested):
            if contested_slot in placed_slots:
                continue
            if not _touches(slots, contested_slot):
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
        default="merge3", description="merge3 (common ancestor) or compose (unrelated)."
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
        description="Declared alignment for a compose; empty for merge3.",
    )
    name_conflict: str | None = PydanticField(
        default=None, description="Compose's name-conflict policy, when applicable."
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
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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

    Returns:
        ``(merged_or_None, result)``, as :func:`merge_three_way`.
    """
    merged, result = merge_three_way(
        base, left, right, resolutions=recipe.resolutions, hints=hints
    )

    recorded = {resolution.slot_key for resolution in recipe.resolutions}
    still_conflicting = {conflict.slot_key for conflict in result.conflicts}
    unused = sorted(recorded - still_conflicting)
    if unused:
        result.warnings.append(
            f"{len(unused)} recorded resolution(s) were not needed this time: "
            + ", ".join("/".join(slot) for slot in unused)
        )
    genuinely_new = sorted(still_conflicting - recorded)
    if genuinely_new:
        result.warnings.append(
            f"{len(genuinely_new)} conflict(s) are new since the recorded merge: "
            + ", ".join("/".join(slot) for slot in genuinely_new)
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
    "build_recipe",
    "describe_slot",
    "find_merge_base",
    "merge_three_way",
    "op_slots",
    "re_merge",
    "take_left",
    "take_right",
]


# Imported for its side effect on the public surface only.
_ = op_to_dict
