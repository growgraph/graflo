"""Recording a compose as a commit.

Deliberately not in ``compose.py``. That module is the pure algorithm: two
manifests in, one out, no notion of a store or of history. Stamping and
recording are what a **commit point** does to the result afterwards -- the same
separation ``contract/provenance.py`` draws to keep provenance out of
``apply_evolution``, and for the same reason. A compose that recorded its own
lineage could not be run twice without inventing two different histories.

A compose commit is materialized exactly as a merge commit is: its ops are the
verified diff from its **first parent**, so first-parent replay and hash
verification need no special case anywhere downstream. What makes it a compose
rather than a merge is the declaration that rides alongside it -- a
:class:`~graflo.architecture.evolution.merge3.MergeRecipe` of kind ``compose``,
which is what a re-compose reads.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest

from .commit import Commit, MergeRecipeRef, build_merge_commit
from .hashing import manifest_hash
from .history import History
from .merge3 import MergeRecipe


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


def build_compose_commit(
    left: GraphManifest,
    composed: GraphManifest,
    *,
    parents: list[str],
    recipe: MergeRecipe,
    label: str | None = None,
    created_at: str | None = None,
    notes: str | None = None,
) -> Commit:
    """Record *composed* as a ``compose`` commit over *parents*.

    Args:
        left: The first parent's manifest -- the side the ops are diffed from.
        composed: The compose result.
        parents: Parent commit ids, first parent first (at least two).
        recipe: The recorded declaration, so a re-compose can replay it.
        label: Short human-readable name.
        created_at: ISO-8601 timestamp.
        notes: Free-form annotation.

    Raises:
        CommitError: Fewer than two parents, or the derived diff does not
            reproduce *composed* -- which happens when the two sides differ
            somewhere no op reaches.
    """
    return build_merge_commit(
        left,
        composed,
        parents=list(parents),
        kind="compose",
        label=label,
        created_at=created_at,
        notes=notes,
        merge_recipe=MergeRecipeRef(
            hash=recipe.content_hash(), kind=recipe.kind, payload=recipe.to_dict()
        ),
    )


__all__ = ["build_compose_commit", "find_commit_by_tree"]
