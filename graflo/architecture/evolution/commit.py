"""Commits: content-addressed change sets with ordered parents.

This replaces the linear ``Revision`` chain. The old model was already "a git
log, not an Alembic script" -- but it still spoke Alembic (``down_revision``,
``upgrade``/``downgrade``) and it could only be a *line*. A world model that can
be merged needs a **DAG**: a commit has a list of parents, empty for a root, one
for an ordinary edit, two or more for a merge or a compose.

What carries over unchanged is the property that made the chain worth having:
each commit records the content hash before and after it, so replay is
*verified* rather than assumed. A history that no longer describes the manifest
it was generated from fails loudly instead of producing a plausible wrong
answer.

Merge commits are **materialized**, git-style
-----------------------------------------------

A merge commit's ``ops`` are the diff from its **first parent's** state to the
merged result -- not some interleaving of both sides. That single decision is
what keeps everything else simple: first-parent replay and hash verification
work identically for edit and merge commits, so ``History`` needs no special
case and neither does the store. The declarative record of *how* the merge was
resolved rides alongside as a :class:`MergeRecipe`, which is what a re-merge
reads. Interleaving two sides' ops within one commit is deliberately not
expressible, and is not needed.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

from .codec import RevisionOp, ops_from_dicts, ops_to_dicts
from .hashing import manifest_hash
from .ops import ManifestOp

logger = logging.getLogger(__name__)

#: Length of the content-derived commit id, in hex characters.
COMMIT_ID_LENGTH = 12

#: What produced a commit. ``root`` names a tree that nothing derived it from;
#: ``edit`` is an ordinary change set; ``merge`` reconciles two descendants of a
#: common ancestor; ``compose`` joins unrelated lineages by declared
#: equivalence; ``revert`` undoes an earlier commit.
CommitKind = str

COMMIT_KINDS: frozenset[str] = frozenset({"root", "edit", "merge", "compose", "revert"})

_SLUG_STRIP = re.compile(r"[^a-z0-9]+")


class CommitError(RuntimeError):
    """A commit is malformed, or a replay did not reproduce a recorded hash."""


def compute_commit_id(ops: list[ManifestOp], parents: list[str]) -> str:
    """Content-derived id: sha256 over the canonical ops plus the parent ids.

    Deterministic on purpose -- the same change set on the same parents always
    yields the same id, so a regenerated commit is recognisably *the same one*
    rather than a duplicate under a fresh random name.

    Parent order is part of the id, because it is part of the meaning: a merge
    materialized against A-then-B is not the commit materialized against
    B-then-A.
    """
    payload = json.dumps(
        {"ops": ops_to_dicts(list(ops)), "parents": list(parents)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:COMMIT_ID_LENGTH]


class Commit(ConfigBaseModel):
    """One change set, its ordered parents, and the trees it moves between."""

    id: str = PydanticField(..., description="Content-derived id of this commit.")
    parents: list[str] = PydanticField(
        default_factory=list,
        description=(
            "Parent commit ids in position order: empty for a root, one for an "
            "edit, two or more for a merge or compose. The first parent is the "
            "one this commit's ops are materialized against."
        ),
    )
    ops: list[RevisionOp] = PydanticField(
        default_factory=list,
        description=(
            "Ordered operations, as a diff from the first parent's tree. Empty "
            "only on a root, which names a tree rather than deriving one."
        ),
    )
    tree_before: str | None = PydanticField(
        default=None,
        description=(
            "Content hash of the first parent's manifest, or ``None`` on a root. "
            "Redundant against the parent's own `tree` on purpose: replay drift "
            "fails here, loudly, rather than producing a plausible wrong manifest."
        ),
    )
    tree: str = PydanticField(
        ..., description="Content hash of the manifest this commit produces."
    )
    kind: str = PydanticField(
        default="edit", description="One of edit / merge / compose / revert."
    )
    label: str | None = PydanticField(
        default=None, description="Short human-readable name."
    )
    created_at: str | None = PydanticField(
        default=None, description="ISO-8601 timestamp, supplied by the caller."
    )
    reversible: bool = PydanticField(
        default=True, description="Whether every op in this commit has a total inverse."
    )
    merge_recipe: MergeRecipeRef | None = PydanticField(
        default=None,
        description="How a merge was resolved; present on merge/compose commits.",
    )
    notes: str | None = None

    @model_validator(mode="after")
    def _only_a_root_may_derive_nothing(self) -> Commit:
        """A commit with a parent must say how it got there.

        Relaxing ``ops`` to allow a root is not a licence for an empty edit: an
        edit whose ops are empty claims a transition it cannot replay, which is
        the one failure the verified-replay design exists to prevent.

        Two parentless shapes are both legal and must not be confused. A
        *derived* root has ops and a ``tree_before`` -- the base a change set was
        authored against. A *naming* root has neither: it asserts that a tree
        exists, which is the only thing that can be said about an artifact that
        simply arrived.
        """
        if self.parents and not self.ops:
            raise ValueError(
                f"commit '{self.id}' has parents but no operations; only a root "
                "commit may name a tree without deriving it"
            )
        if not self.ops and self.tree_before is not None:
            raise ValueError(
                f"commit '{self.id}' derives nothing, so it has no tree to start "
                "from; a naming root carries a tree and no tree_before"
            )
        return self

    @property
    def is_root(self) -> bool:
        """Whether this commit has no parent."""
        return not self.parents

    @property
    def is_multi_parent(self) -> bool:
        """Whether this commit joins two or more lineages.

        True for both `merge` and `compose` kinds: joining unrelated lineages
        is as multi-parent as reconciling related ones. Not a test for
        `kind == "merge"` -- read `kind` for that.
        """
        return len(self.parents) > 1

    @property
    def first_parent(self) -> str | None:
        """The parent this commit's ops are materialized against."""
        return self.parents[0] if self.parents else None

    @property
    def slug(self) -> str:
        """Filesystem-safe label fragment used in the stored filename."""
        base = _SLUG_STRIP.sub("_", (self.label or "commit").lower()).strip("_")
        return base or "commit"

    def short(self) -> str:
        """The id, abbreviated the way a log line shows it."""
        return self.id[:8]


class MergeRecipeRef(ConfigBaseModel):
    """A pointer to the recorded merge recipe, plus its content hash.

    The recipe model itself lives in ``merge3`` (L4, same layer) and is stored
    inline on the commit. This indirection keeps ``commit.py`` importable
    without pulling the merge machinery in, which matters because the store and
    the history DAG only ever need the *hash*.
    """

    hash: str = PydanticField(..., description="Content hash of the recipe.")
    kind: str = PydanticField(
        default="merge3", description="Recipe flavour: merge3 or compose."
    )
    payload: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="The serialized recipe, as produced by MergeRecipe.to_dict().",
    )


Commit.model_rebuild()


def compute_root_commit_id(tree: str, scope: str | None = None) -> str:
    """Content-derived id for a root that names *tree*.

    :func:`compute_commit_id` hashes the ops and the parents, both of which are
    empty on a naming root -- so every root in existence would share one id.
    The tree is what a root asserts, so the tree is what identifies it.

    *scope* separates two roots that name the same tree but belong to different
    lineages. A registry keyed by artifact passes the artifact id: without it,
    two artifacts whose first version has identical content collide, the store
    dedupes the second against the first, and one of them silently ends up with
    no root at all.
    """
    payload = json.dumps(
        {"root": tree, "scope": scope}, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:COMMIT_ID_LENGTH]


def build_root_commit(
    manifest: GraphManifest,
    *,
    scope: str | None = None,
    label: str | None = None,
    created_at: str | None = None,
    notes: str | None = None,
) -> Commit:
    """Record *manifest* as a root that names its tree.

    The counterpart to :func:`build_commit` for an artifact that was not derived
    from anything this history holds -- pushed to a registry, seeded from a
    pack, or otherwise simply present. There is no base to diff against, so
    there are no ops: the commit asserts the tree and stops.

    That is deliberately not "a diff from an empty manifest". An empty
    ``GraphManifest`` is not constructible (it must carry at least one block),
    and the op vocabulary cannot build every block from nothing -- so a
    synthesized construction diff would fail for exactly the manifests a
    registry most often holds.

    Args:
        manifest: The artifact this root names.
        scope: Lineage discriminator folded into the id; see
            :func:`compute_root_commit_id`.
        label: Short human-readable name.
        created_at: ISO-8601 timestamp.
        notes: Free-form annotation.
    """
    tree = manifest_hash(manifest)
    return Commit(
        id=compute_root_commit_id(tree, scope),
        parents=[],
        ops=[],
        tree_before=None,
        tree=tree,
        kind="root",
        label=label,
        created_at=created_at,
        reversible=True,
        notes=notes,
    )


def build_commit(
    base: GraphManifest,
    ops: list[ManifestOp],
    *,
    parents: list[str] | None = None,
    kind: str = "edit",
    label: str | None = None,
    created_at: str | None = None,
    notes: str | None = None,
    merge_recipe: MergeRecipeRef | None = None,
) -> Commit:
    """Apply *ops* to *base* and record the result as a :class:`Commit`.

    The ops are applied here rather than trusted, so both trees describe a
    transition that actually happened. Refuses an empty change set and one that
    leaves the manifest unchanged -- a commit that moves nothing is a lie about
    history, not a harmless no-op.

    Args:
        base: The first parent's manifest state.
        ops: The change set to apply.
        parents: Parent commit ids, first parent first.
        kind: One of ``edit``, ``merge``, ``compose``, ``revert``.
        label: Short human-readable name.
        created_at: ISO-8601 timestamp.
        notes: Free-form annotation.
        merge_recipe: Recorded resolution, on merge and compose commits.

    Raises:
        CommitError: The change set is empty, is a no-op, or *kind* is unknown.
    """
    from .apply import apply_evolution
    from .inverse import irreversible_reason

    if not ops:
        raise CommitError("a commit needs at least one operation")
    if kind not in COMMIT_KINDS:
        raise CommitError(
            f"unknown commit kind '{kind}'; expected one of {sorted(COMMIT_KINDS)}"
        )

    parent_ids = list(parents or [])
    before = manifest_hash(base)
    applied = apply_evolution(base, ops, bump_version=False, finish_init=False)
    after = manifest_hash(applied)
    if after == before:
        raise CommitError(
            "the operations leave the manifest unchanged; there is nothing to record"
        )

    return Commit(
        id=compute_commit_id(ops, parent_ids),
        parents=parent_ids,
        # Round-tripped through the codec on the way in, so a commit can only
        # ever hold ops that survive serialization -- the property replay
        # depends on. Also rejects the binary compose op, which no single
        # manifest transition can express.
        ops=ops_from_dicts(ops_to_dicts(list(ops))),
        tree_before=before,
        tree=after,
        kind=kind,
        label=label,
        created_at=created_at,
        reversible=all(irreversible_reason(op) is None for op in ops),
        merge_recipe=merge_recipe,
        notes=notes,
    )


def build_merge_commit(
    first_parent: GraphManifest,
    merged: GraphManifest,
    *,
    parents: list[str],
    kind: str = "merge",
    label: str | None = None,
    created_at: str | None = None,
    notes: str | None = None,
    merge_recipe: MergeRecipeRef | None = None,
) -> Commit:
    """Record a merge as the diff from its **first parent** to *merged*.

    This is what makes a merge commit replayable by exactly the same machinery
    as an edit: the stored ops move first-parent → result, so first-parent
    replay and hash verification need no special case anywhere downstream.

    The diff is verified before it is stored -- if replaying the derived ops
    does not reproduce *merged*, that is raised here rather than written into
    history.

    Args:
        first_parent: Manifest state of the parent the ops are diffed from.
        merged: The merge result.
        parents: All parent commit ids, first parent first (at least two).
        kind: ``merge`` or ``compose``.
        label: Short human-readable name.
        created_at: ISO-8601 timestamp.
        notes: Free-form annotation.
        merge_recipe: The recorded resolution, so a re-merge can replay it.

    Raises:
        CommitError: Fewer than two parents, or the derived diff does not
            reproduce *merged*.
    """
    from .autogenerate import diff_manifests_verified

    if len(parents) < 2:
        raise CommitError(
            f"a {kind} commit needs at least two parents, got {len(parents)}"
        )

    ops, warnings = diff_manifests_verified(first_parent, merged)
    if not ops:
        raise CommitError(
            "the merge result is identical to its first parent; there is "
            "nothing to record"
        )
    if warnings:
        raise CommitError(
            "the derived merge diff does not fully reproduce the merged "
            "manifest, so it would not replay: " + "; ".join(warnings)
        )

    return build_commit(
        first_parent,
        ops,
        parents=list(parents),
        kind=kind,
        label=label,
        created_at=created_at,
        notes=notes,
        merge_recipe=merge_recipe,
    )


def build_revert_commit(
    current: GraphManifest,
    commit: Commit,
    *,
    parents: list[str],
    label: str | None = None,
    created_at: str | None = None,
) -> Commit:
    """A new commit that undoes *commit*, applied on top of *current*.

    Git-shaped, and deliberately not a "downgrade": history is append-only, so
    undoing a change is a new commit that moves forward, never an edit to what
    was recorded. Everything downstream -- replay, verification, the DAG -- then
    needs no notion of rewinding at all.

    Inversion is exact or it fails. An op with no total inverse, or one whose
    inverse needs data the current manifest no longer holds (restoring a removed
    vertex, say), raises rather than producing a manifest that merely *resembles*
    the earlier state. When the base manifest is available, checking out the
    parent commit is always exact and is the better tool.

    Args:
        current: The manifest to apply the reverting ops to.
        commit: The commit being undone.
        parents: Parent commit ids for the new commit -- normally the head.
        label: Short human-readable name.
        created_at: ISO-8601 timestamp.

    Raises:
        CommitError: The commit is not invertible from *current* alone.
    """
    from .inverse import invert_op, irreversible_reason

    blockers = [
        f"{op.op} ({irreversible_reason(op)})"
        for op in commit.ops
        if irreversible_reason(op) is not None
    ]
    if blockers:
        raise CommitError(
            f"commit '{commit.short()}' cannot be reverted -- these operations "
            "are irreversible: " + "; ".join(blockers) + ". Check out the parent "
            "commit from the base manifest instead."
        )

    # Walking backwards: the running manifest is the state *after* each op,
    # which is all that exists without a base.
    inverses: list[ManifestOp] = []
    probe = current
    from .apply import apply_evolution

    for op in reversed(list(commit.ops)):
        inverse = invert_op(op, manifest=probe)
        if inverse is None:
            raise CommitError(
                f"operation '{op.op}' in commit '{commit.short()}' cannot be "
                "inverted from the current manifest alone -- the data it would "
                "restore is no longer present. Check out from the base instead."
            )
        inverses.append(inverse)
        probe = apply_evolution(probe, [inverse], bump_version=False, finish_init=False)

    return build_commit(
        current,
        inverses,
        parents=list(parents),
        kind="revert",
        label=label or f"revert {commit.short()}",
        created_at=created_at,
    )


__all__ = [
    "COMMIT_ID_LENGTH",
    "COMMIT_KINDS",
    "Commit",
    "CommitError",
    "CommitKind",
    "MergeRecipeRef",
    "build_commit",
    "build_merge_commit",
    "build_revert_commit",
    "compute_commit_id",
]
