"""Recording a merge as a commit.

Deliberately not in ``merge.py``. That module is the pure algorithm: two
manifests in, one out, no notion of a store or of history. Stamping and
recording are what a **commit point** does to the result afterwards -- the same
separation ``contract/provenance.py`` draws to keep provenance out of
``apply_evolution``, and for the same reason. A merge that recorded its own
lineage could not be run twice without inventing two different histories.

A merge commit is materialized exactly as a merge commit is: its ops are the
verified diff from its **first parent**, so first-parent replay and hash
verification need no special case anywhere downstream. What makes it a merge
rather than a merge is the declaration that rides alongside it -- a
:class:`~graflo.architecture.evolution.merge3.MergeRecipe` of kind ``merge``,
which is what a re-merge reads.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest

from .commit import Commit, CommitError, MergeRecipeRef, build_multi_parent_commit
from .hashing import manifest_hash
from .history import History
from .merge3 import MergeRecipe
from .ops import ManifestOp


def find_commit_by_tree(history: History, manifest: GraphManifest) -> Commit | None:
    """The commit whose recorded ``tree`` is this manifest's content address.

    How a file path becomes a commit id without the caller naming one: a
    manifest *is* its content hash, and a commit records the tree it produced,
    so the two meet without any extra bookkeeping.

    More than one commit can legitimately reach one tree -- two routes to the
    same world model are the case content addressing exists to recognise. When
    that happens a head wins, because that is the state someone is working from;
    failing that the lowest id, so the answer never depends on store order.
    """
    tree = manifest_hash(manifest)
    candidates = [commit for commit in history.commits if commit.tree == tree]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    heads = {head.id for head in history.heads()}
    preferred = [commit for commit in candidates if commit.id in heads] or candidates
    return min(preferred, key=lambda commit: commit.id)


def left_relabel_ops(
    left: GraphManifest, right: GraphManifest, recipe: MergeRecipe
) -> list[ManifestOp]:
    """The relabel a merge applied to its left side, as ops.

    ``merge_manifests`` resolves its declared clusters and canonical maps into
    one composite ``canonicalize`` per side and applies it before the union.
    The left one is part of how the merged manifest came from the left, so a
    merge commit records it first. Resolved from the recorded declaration --
    the whole op, canonical maps included -- against both sides, exactly as
    the merge resolved it.
    """
    from .canonical import canonicalize_ops, resolve_clusters
    from .ops import MergeManifestsOp

    if recipe.kind != "merge":
        return []
    op = MergeManifestsOp.model_validate(recipe.equivalences)
    try:
        resolution = resolve_clusters(
            op, left=left.model_copy(deep=True), right=right.model_copy(deep=True)
        )
    except Exception as exc:
        raise CommitError(
            f"the recorded merge declaration does not resolve against its inputs: {exc}"
        ) from exc
    return canonicalize_ops(resolution.side_maps["left"])


def build_merge_commit(
    left: GraphManifest,
    merged: GraphManifest,
    *,
    parents: list[str],
    recipe: MergeRecipe,
    right: GraphManifest | None = None,
    label: str | None = None,
    created_at: str | None = None,
    notes: str | None = None,
) -> Commit:
    """Record *merged* as a ``merge`` commit over *parents*.

    Args:
        left: The first parent's manifest -- the side the ops are diffed from.
        merged: The merge result.
        parents: Parent commit ids, first parent first (at least two).
        recipe: The recorded declaration, so a re-merge can replay it.
        right: The second input. When given, the relabel the merge applied to
            *left* is recorded ahead of the diff (see :func:`left_relabel_ops`);
            without it a merge that renamed or folded a left class can only be
            diffed as an unrelated add and remove, which rarely replays.
        label: Short human-readable name.
        created_at: ISO-8601 timestamp.
        notes: Free-form annotation.

    Raises:
        CommitError: Fewer than two parents, or the derived diff does not
            reproduce *merged* -- which happens when the two sides differ
            somewhere no op reaches.
    """
    return build_multi_parent_commit(
        left,
        merged,
        parents=list(parents),
        kind="merge",
        label=label,
        created_at=created_at,
        notes=notes,
        merge_recipe=MergeRecipeRef(
            hash=recipe.content_hash(), kind=recipe.kind, payload=recipe.to_dict()
        ),
        lead_ops=left_relabel_ops(left, right, recipe) if right is not None else None,
    )


__all__ = ["build_merge_commit", "find_commit_by_tree"]
