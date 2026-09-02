"""The commit DAG, and commits on disk.

``RevisionChain`` could only be a line, and it *raised* when two commits shared
a parent. That is the wrong response: a fork is something that happened, and a
history that refuses to represent it cannot be the record of what happened. The
server had the same defect from the other direction -- it silently dropped a
recording that did not extend the head, so a fork was simply lost.

Here a fork is a recorded fact. :class:`History` validates the things that are
genuinely broken -- duplicate ids, a parent that does not exist, a first-parent
edge whose trees do not line up -- and represents everything else, including
multiple heads.

Linear histories stay easy: :meth:`History.linearize` returns the single path
when there is one, and ``checkout`` replays first-parent edges, which is the
same walk the old chain did.
"""

from __future__ import annotations

import logging
from collections import deque
from pathlib import Path
from typing import Any

from pydantic import Field as PydanticField
from pydantic import model_validator
from suthing import FileHandle

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

from .codec import ops_from_dicts, ops_to_dicts
from .commit import Commit, CommitError
from .hashing import manifest_hash

logger = logging.getLogger(__name__)


class History(ConfigBaseModel):
    """A set of commits forming a directed acyclic graph."""

    commits: list[Commit] = PydanticField(default_factory=list)

    @model_validator(mode="after")
    def _validate_dag(self) -> History:
        if not self.commits:
            return self

        ids = [commit.id for commit in self.commits]
        duplicates = sorted({cid for cid in ids if ids.count(cid) > 1})
        if duplicates:
            raise ValueError(f"duplicate commit ids: {duplicates}")

        known = set(ids)
        by_id = {commit.id: commit for commit in self.commits}

        for commit in self.commits:
            missing = [pid for pid in commit.parents if pid not in known]
            if missing:
                raise ValueError(
                    f"commit '{commit.id}' names parents not in this history: {missing}"
                )

        # Cycles first: they are the more fundamental defect, and a cycle makes
        # every other property meaningless (`ancestors` would not terminate, and
        # "the parent's tree" is not well defined when a commit is its own
        # ancestor). Reporting a tree mismatch for what is really a loop sends
        # the reader looking for drift that is not there.
        self._reject_cycles(by_id)

        for commit in self.commits:
            # Tree continuity along the first-parent edge only. Second parents
            # deliberately carry no such constraint -- a merge's ops are the
            # diff from its *first* parent, so the second parent's tree has no
            # reason to line up with anything here.
            parent_id = commit.first_parent
            if parent_id is not None:
                parent = by_id[parent_id]
                if commit.tree_before != parent.tree:
                    raise ValueError(
                        f"commit '{commit.id}' expects to start from tree "
                        f"{commit.tree_before[:12]} but its first parent "
                        f"'{parent_id}' produces {parent.tree[:12]}"
                    )
        return self

    @staticmethod
    def _reject_cycles(by_id: dict[str, Commit]) -> None:
        """A cycle makes 'ancestors of X' non-terminating; refuse to hold one."""
        # Kahn's algorithm: a DAG drains completely, a cycle leaves a residue.
        indegree = {cid: len(commit.parents) for cid, commit in by_id.items()}
        children: dict[str, list[str]] = {cid: [] for cid in by_id}
        for cid, commit in by_id.items():
            for parent in commit.parents:
                children[parent].append(cid)

        queue = deque(cid for cid, degree in indegree.items() if degree == 0)
        drained = 0
        while queue:
            current = queue.popleft()
            drained += 1
            for child in children[current]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if drained != len(by_id):
            cyclic = sorted(cid for cid, degree in indegree.items() if degree > 0)
            raise ValueError(f"commit history contains a cycle involving: {cyclic}")

    # ── lookup ──────────────────────────────────────────────────────────────

    def get(self, commit_id: str) -> Commit | None:
        """The commit with *commit_id*, or ``None``.

        Accepts an unambiguous prefix, the way git does.
        """
        for commit in self.commits:
            if commit.id == commit_id:
                return commit
        matches = [c for c in self.commits if c.id.startswith(commit_id)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise CommitError(
                f"'{commit_id}' is ambiguous: {sorted(c.id for c in matches)}"
            )
        return None

    def require(self, commit_id: str) -> Commit:
        """Like :meth:`get`, but raises when there is no such commit."""
        commit = self.get(commit_id)
        if commit is None:
            raise CommitError(f"unknown commit '{commit_id}'")
        return commit

    def heads(self) -> list[Commit]:
        """Commits no other commit names as a parent, oldest-first by topology.

        More than one head means the history has forked -- a fact to report,
        not an error to raise.
        """
        referenced = {pid for commit in self.commits for pid in commit.parents}
        order = {commit.id: index for index, commit in enumerate(self.topological())}
        heads = [c for c in self.commits if c.id not in referenced]
        return sorted(heads, key=lambda c: order.get(c.id, 0))

    def roots(self) -> list[Commit]:
        """Commits with no parent."""
        return [commit for commit in self.commits if commit.is_root]

    def children_of(self, commit_id: str) -> list[Commit]:
        """Commits naming *commit_id* as any of their parents."""
        return [c for c in self.commits if commit_id in c.parents]

    # ── traversal ───────────────────────────────────────────────────────────

    def ancestors(self, commit_id: str, *, include_self: bool = False) -> set[str]:
        """Every commit reachable by following parents from *commit_id*."""
        start = self.require(commit_id)
        seen: set[str] = {start.id} if include_self else set()
        queue = deque(start.parents)
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            queue.extend(self.require(current).parents)
        return seen

    def topological(self) -> list[Commit]:
        """All commits, parents before children, deterministic on ties.

        Ties break on commit id, so the same history always linearizes the same
        way -- a log that reorders itself between runs is not a log.
        """
        by_id = {commit.id: commit for commit in self.commits}
        indegree = {cid: len(commit.parents) for cid, commit in by_id.items()}
        children: dict[str, list[str]] = {cid: [] for cid in by_id}
        for cid, commit in by_id.items():
            for parent in commit.parents:
                children[parent].append(cid)

        ready = sorted(cid for cid, degree in indegree.items() if degree == 0)
        ordered: list[Commit] = []
        while ready:
            current = ready.pop(0)
            ordered.append(by_id[current])
            for child in sorted(children[current]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
            ready.sort()
        return ordered

    def first_parent_path(self, commit_id: str) -> list[Commit]:
        """Root → *commit_id* along first-parent edges, oldest first.

        This is the walk a checkout replays. Following only first parents is
        what makes it work for merge commits too: their ops were materialized
        against exactly this parent.
        """
        path: list[Commit] = []
        current: str | None = commit_id
        while current is not None:
            commit = self.require(current)
            path.append(commit)
            current = commit.first_parent
        path.reverse()
        return path

    def linearize(self) -> list[Commit]:
        """The single path through a linear history, oldest first.

        Raises:
            CommitError: The history has forked or has several roots, so there
                is no single path. Use :meth:`topological` for the general case.
        """
        heads = self.heads()
        if len(heads) > 1:
            raise CommitError(
                "history has forked into "
                f"{len(heads)} heads ({', '.join(h.short() for h in heads)}); "
                "there is no single path. Use topological() or name a head."
            )
        roots = self.roots()
        if len(roots) > 1:
            raise CommitError(
                f"history has {len(roots)} roots; there is no single path."
            )
        if not heads:
            return []
        return self.first_parent_path(heads[0].id)

    @property
    def reversible(self) -> bool:
        """Whether every commit in the history can be inverted."""
        return all(commit.reversible for commit in self.commits)

    def extend(self, commit: Commit) -> History:
        """A new history with *commit* added; validation re-runs on the copy."""
        return History(commits=[*self.commits, commit])


def checkout(
    base: GraphManifest,
    history: History,
    commit_id: str | None = None,
    *,
    verify: bool = True,
    finish_init: bool = False,
) -> GraphManifest:
    """The manifest as of *commit_id*, replayed from *base*.

    Replays first-parent edges from the root, verifying every recorded tree on
    the way. Verification is the point: a history that no longer describes the
    manifest it was generated from fails here instead of producing a plausible
    but wrong result.

    Args:
        base: The manifest the root commit starts from.
        history: The commit DAG.
        commit_id: Where to stop. ``None`` means the single head, and raises if
            the history has forked.
        verify: Check each recorded tree hash against the replayed manifest.
        finish_init: Run ``finish_init`` after each applied change set.

    Raises:
        CommitError: An unknown commit, a forked history with no commit named,
            or a replay that did not reproduce a recorded tree.
    """
    from .apply import apply_evolution

    if commit_id is None:
        heads = history.heads()
        if not heads:
            return base
        if len(heads) > 1:
            raise CommitError(
                "history has forked into "
                f"{len(heads)} heads ({', '.join(h.short() for h in heads)}); "
                "name the commit to check out"
            )
        commit_id = heads[0].id

    current = base
    for commit in history.first_parent_path(commit_id):
        if verify:
            actual = manifest_hash(current)
            if actual != commit.tree_before:
                raise CommitError(
                    f"commit '{commit.id}' expects to start from tree "
                    f"{commit.tree_before[:12]} but the manifest hashes "
                    f"{actual[:12]}; the base or an earlier commit has drifted"
                )
        current = apply_evolution(
            current, commit.ops, bump_version=False, finish_init=finish_init
        )
        if verify:
            produced = manifest_hash(current)
            if produced != commit.tree:
                raise CommitError(
                    f"replaying commit '{commit.id}' produced tree "
                    f"{produced[:12]}, not the recorded {commit.tree[:12]}"
                )
    return current


def verify_history(base: GraphManifest, history: History) -> list[str]:
    """Replay every head and report what fails, instead of raising on the first.

    Returns a list of human-readable problems -- empty when the whole DAG
    replays cleanly. Useful as a health check over a history that may have
    several heads, where ``checkout`` would refuse to pick one.
    """
    problems: list[str] = []
    for head in history.heads():
        try:
            checkout(base, history, head.id)
        except CommitError as exc:
            problems.append(f"head {head.short()}: {exc}")
    return problems


class FileCommitStore:
    """Commits on disk, one YAML file per commit.

    Filenames carry a topological index so a directory listing reads roughly in
    order, but the index is **not** authoritative: the loader rebuilds the DAG
    from the recorded parent ids. That is the difference from the old store,
    which reconstructed a chain by walking a linear parent map and raised the
    moment two commits shared a parent.
    """

    def __init__(self, root: str | Path = ".graflo/commits") -> None:
        self.root = Path(root)

    def load(self) -> History:
        """Read every stored commit and assemble the DAG."""
        if not self.root.exists():
            return History()
        payloads = [FileHandle.load(path) for path in sorted(self.root.glob("*.yaml"))]
        return History(commits=[_commit_from_dict(payload) for payload in payloads])

    def save(self, history: History) -> list[Path]:
        """Write *history*, replacing whatever was there."""
        self.root.mkdir(parents=True, exist_ok=True)
        for stale in self.root.glob("*.yaml"):
            stale.unlink()
        written: list[Path] = []
        for index, commit in enumerate(history.topological()):
            path = self.root / f"{index:04d}_{commit.id}_{commit.slug}.yaml"
            FileHandle.dump(_commit_to_dict(commit), path)
            written.append(path)
        return written

    def append(self, commit: Commit) -> Path:
        """Add one commit to the stored history, validating the DAG."""
        history = self.load().extend(commit)
        self.save(history)
        return next(path for path in self.root.glob("*.yaml") if commit.id in path.name)


def _commit_to_dict(commit: Commit) -> dict[str, Any]:
    payload = commit.to_dict(skip_defaults=True)
    # `ops` needs the codec's verified serialization, not a plain dump: a
    # nested discriminator with a default would otherwise be dropped.
    payload["ops"] = ops_to_dicts(list(commit.ops))
    payload["id"] = commit.id
    payload["parents"] = list(commit.parents)
    return payload


def _commit_from_dict(payload: dict[str, Any]) -> Commit:
    data = dict(payload)
    data["ops"] = ops_from_dicts(list(data.get("ops", [])))
    return Commit.model_validate(data)


__all__ = [
    "FileCommitStore",
    "History",
    "checkout",
    "verify_history",
]
