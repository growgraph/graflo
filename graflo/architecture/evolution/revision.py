"""Forward-only revision chains over a base manifest.

Alembic's core abstraction is a reversible ``upgrade()`` / ``downgrade()`` pair.
GraFlo cannot offer that: several ops are lossy (see
:data:`~graflo.architecture.evolution.inverse.IRREVERSIBLE`), and a ``downgrade``
that silently produces a *different* manifest is worse than none.

So the model here is a **git log**, not an Alembic script: an ordered chain of
content-hashed change sets over a base manifest. Going back means replaying the
chain from the base up to the revision you want, which is always correct.
Inverses are used only when no base is available, and only when every op on the
path has one.

Each revision records the manifest hash before and after it, so replay is
*verified* rather than assumed. That is the property the existing
``MigrationRecord`` cannot offer — it stores bare op-type names, dropping the
targets and values a replay would need.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

from pydantic import Field as PydanticField
from pydantic import model_validator
from suthing import FileHandle

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

from .codec import RevisionOp, ops_from_dicts, ops_to_dicts
from .hashing import manifest_hash
from .inverse import invert_op, irreversible_reason
from .ops import ManifestOp

logger = logging.getLogger(__name__)

#: Length of the content-derived revision id, in hex characters.
REVISION_ID_LENGTH = 12

_SLUG_STRIP = re.compile(r"[^a-z0-9]+")


class RevisionError(RuntimeError):
    """A chain is malformed, or a replay did not reproduce a recorded hash."""


def compute_revision_id(ops: list[ManifestOp], down_revision: str | None) -> str:
    """Content-derived id: sha256 over the canonical ops plus the parent id.

    Deterministic on purpose — the same change set on the same parent always
    yields the same id, so a re-generated revision is recognisably the same one
    rather than a duplicate with a fresh random name.
    """
    payload = json.dumps(
        {"ops": ops_to_dicts(list(ops)), "down_revision": down_revision},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:REVISION_ID_LENGTH]


class Revision(ConfigBaseModel):
    """One change set, its parent, and the hashes it moves between."""

    revision: str = PydanticField(
        ..., description="Content-derived id of this revision."
    )
    down_revision: str | None = PydanticField(
        default=None,
        description="Parent revision id; ``None`` marks the first revision.",
    )
    label: str | None = PydanticField(
        default=None, description="Short human-readable name."
    )
    created_at: str | None = PydanticField(
        default=None, description="ISO-8601 timestamp, supplied by the caller."
    )
    ops: list[RevisionOp] = PydanticField(
        ..., min_length=1, description="Ordered operations this revision applies."
    )
    manifest_hash_before: str = PydanticField(
        ..., description="Manifest hash this revision expects to start from."
    )
    manifest_hash_after: str = PydanticField(
        ..., description="Manifest hash this revision produces."
    )
    reversible: bool = PydanticField(
        default=True,
        description="Whether every op in this revision has a total inverse.",
    )
    notes: str | None = None

    @property
    def slug(self) -> str:
        """Filesystem-safe label fragment used in the stored filename."""
        base = _SLUG_STRIP.sub("_", (self.label or "revision").lower()).strip("_")
        return base or "revision"


def build_revision(
    base: GraphManifest,
    ops: list[ManifestOp],
    *,
    down_revision: str | None = None,
    label: str | None = None,
    created_at: str | None = None,
    notes: str | None = None,
) -> Revision:
    """Apply *ops* to *base* and record the result as a :class:`Revision`.

    The ops are applied here rather than trusted, so both hashes describe a
    transition that actually happened.
    """
    from .apply import apply_evolution

    if not ops:
        raise RevisionError("a revision needs at least one operation")

    before = manifest_hash(base)
    applied = apply_evolution(base, ops, bump_version=False, finish_init=False)
    after = manifest_hash(applied)
    if after == before:
        raise RevisionError(
            "the operations leave the manifest unchanged; there is nothing to record"
        )

    return Revision(
        revision=compute_revision_id(ops, down_revision),
        down_revision=down_revision,
        label=label,
        created_at=created_at,
        # Round-tripped through the codec on the way in, so a revision can only
        # ever hold ops that survive serialization — the property replay
        # depends on. Also rejects the binary compose op, which no single
        # manifest transition can express.
        ops=ops_from_dicts(ops_to_dicts(list(ops))),
        manifest_hash_before=before,
        manifest_hash_after=after,
        reversible=all(irreversible_reason(op) is None for op in ops),
        notes=notes,
    )


class RevisionChain(ConfigBaseModel):
    """An ordered, singly-linked chain of revisions.

    Linear in v1: branching would need a merge policy for two change sets over
    the same parent, which is a separate design question.
    """

    revisions: list[Revision] = PydanticField(default_factory=list)

    @model_validator(mode="after")
    def _validate_links(self) -> RevisionChain:
        if not self.revisions:
            return self

        ids = [revision.revision for revision in self.revisions]
        duplicates = {rev for rev in ids if ids.count(rev) > 1}
        if duplicates:
            raise ValueError(f"duplicate revision ids: {sorted(duplicates)}")

        if self.revisions[0].down_revision is not None:
            raise ValueError(
                f"the first revision '{ids[0]}' points at parent "
                f"'{self.revisions[0].down_revision}', which is not in this chain"
            )
        for previous, current in zip(self.revisions, self.revisions[1:]):
            if current.down_revision != previous.revision:
                raise ValueError(
                    f"revision '{current.revision}' points at parent "
                    f"'{current.down_revision}', but follows '{previous.revision}'; "
                    "the chain must be linear"
                )
            if current.manifest_hash_before != previous.manifest_hash_after:
                raise ValueError(
                    f"revision '{current.revision}' expects to start from hash "
                    f"{current.manifest_hash_before[:12]} but '{previous.revision}' "
                    f"produces {previous.manifest_hash_after[:12]}"
                )
        return self

    def head(self) -> Revision | None:
        """The most recent revision, or ``None`` for an empty chain."""
        return self.revisions[-1] if self.revisions else None

    def get(self, revision_id: str) -> Revision | None:
        """The revision with *revision_id*, or ``None``."""
        for revision in self.revisions:
            if revision.revision == revision_id:
                return revision
        return None

    def path_to(self, revision_id: str | None) -> list[Revision]:
        """Revisions from the base up to and including *revision_id*.

        ``None`` means the whole chain.
        """
        if revision_id is None:
            return list(self.revisions)
        path: list[Revision] = []
        for revision in self.revisions:
            path.append(revision)
            if revision.revision == revision_id:
                return path
        raise RevisionError(f"unknown revision '{revision_id}'")

    def extend(self, revision: Revision) -> RevisionChain:
        """A new chain with *revision* appended; validation re-runs on the copy."""
        return RevisionChain(revisions=[*self.revisions, revision])

    @property
    def reversible(self) -> bool:
        """Whether every revision in the chain can be inverted."""
        return all(revision.reversible for revision in self.revisions)


def apply_revisions(
    base: GraphManifest,
    chain: RevisionChain,
    *,
    upto: str | None = None,
    verify: bool = True,
    finish_init: bool = False,
) -> GraphManifest:
    """Replay *chain* onto *base*, verifying each recorded hash on the way.

    Verification is the point: a chain that no longer describes the manifest it
    was generated from fails here, loudly, instead of producing a plausible but
    wrong result.
    """
    from .apply import apply_evolution

    current = base
    for revision in chain.path_to(upto):
        if verify:
            actual = manifest_hash(current)
            if actual != revision.manifest_hash_before:
                raise RevisionError(
                    f"revision '{revision.revision}' expects to start from hash "
                    f"{revision.manifest_hash_before[:12]} but the manifest hashes "
                    f"{actual[:12]}; the base or an earlier revision has drifted"
                )
        current = apply_evolution(
            current, revision.ops, bump_version=False, finish_init=finish_init
        )
        if verify:
            produced = manifest_hash(current)
            if produced != revision.manifest_hash_after:
                raise RevisionError(
                    f"replaying revision '{revision.revision}' produced hash "
                    f"{produced[:12]}, not the recorded {revision.manifest_hash_after[:12]}"
                )
    return current


def downgrade_to(
    chain: RevisionChain,
    target_revision: str | None,
    *,
    base: GraphManifest | None = None,
    current: GraphManifest | None = None,
) -> GraphManifest:
    """The manifest as of *target_revision*.

    ``target_revision=None`` means the state *before* the first revision — the
    base. (Deliberately the opposite of :func:`apply_revisions`'s ``upto=None``,
    which applies the whole chain; here you are naming where to stop going back.)

    Prefers replaying from *base*, which is correct for every chain. Falls back
    to applying inverses to *current* only when no base is available, and raises
    when any op on the path back is irreversible rather than returning a
    manifest that merely resembles the original.
    """
    if base is not None:
        # ``target_revision=None`` means "the state before any revision" — the
        # base itself. Note this is the opposite of ``apply_revisions(upto=None)``,
        # which means "apply the whole chain".
        if target_revision is None:
            return base
        return apply_revisions(base, chain, upto=target_revision)

    if current is None:
        raise RevisionError(
            "downgrade needs either the base manifest (preferred, always exact) "
            "or the current one to invert from"
        )

    undo = _revisions_after(chain, target_revision)
    if not undo:
        return current

    blockers = [
        f"{revision.revision}: {op.op} ({irreversible_reason(op)})"
        for revision in undo
        for op in revision.ops
        if irreversible_reason(op) is not None
    ]
    if blockers:
        raise RevisionError(
            "cannot downgrade by inversion — these operations are irreversible: "
            + "; ".join(blockers)
            + ". Replay from the base manifest instead."
        )

    from .apply import apply_evolution

    manifest = current
    for revision in reversed(undo):
        for op in reversed(list(revision.ops)):
            # Walking backwards, the running manifest is the state *after* this
            # op — which is all that exists without a base. An inverse needing
            # the pre-state (restoring a removed vertex, say) cannot find its
            # data there and returns None, which is reported rather than
            # papered over.
            inverse = invert_op(op, manifest=manifest)
            if inverse is None:
                raise RevisionError(
                    f"revision '{revision.revision}': operation '{op.op}' cannot "
                    "be inverted from the current manifest alone — the data it "
                    "would restore is no longer present. Replay from the base "
                    "manifest instead."
                )
            manifest = apply_evolution(
                manifest, [inverse], bump_version=False, finish_init=False
            )
        if manifest_hash(manifest) != revision.manifest_hash_before:
            raise RevisionError(
                f"inverting revision '{revision.revision}' produced hash "
                f"{manifest_hash(manifest)[:12]}, not the recorded "
                f"{revision.manifest_hash_before[:12]}; the inversion is not exact"
            )
    return manifest


def _revisions_after(
    chain: RevisionChain, target_revision: str | None
) -> list[Revision]:
    if target_revision is None:
        return list(chain.revisions)
    path = chain.path_to(target_revision)
    return chain.revisions[len(path) :]


class FileRevisionStore:
    """Revisions on disk, one YAML file per revision.

    Mirrors ``migrate.store.FileMigrationStore`` in shape. Filenames are
    ``<index>_<revision>_<slug>.yaml`` so a directory listing reads in order.
    """

    def __init__(self, root: str | Path = ".graflo/revisions") -> None:
        self.root = Path(root)

    def load(self) -> RevisionChain:
        """Read every stored revision and link them into a chain."""
        if not self.root.exists():
            return RevisionChain()
        payloads = [FileHandle.load(path) for path in sorted(self.root.glob("*.yaml"))]
        revisions = [_revision_from_dict(payload) for payload in payloads]
        return RevisionChain(revisions=_link_order(revisions))

    def save(self, chain: RevisionChain) -> list[Path]:
        """Write *chain*, replacing whatever was there."""
        self.root.mkdir(parents=True, exist_ok=True)
        for stale in self.root.glob("*.yaml"):
            stale.unlink()
        written: list[Path] = []
        for index, revision in enumerate(chain.revisions):
            path = self.root / f"{index:04d}_{revision.revision}_{revision.slug}.yaml"
            FileHandle.dump(_revision_to_dict(revision), path)
            written.append(path)
        return written

    def append(self, revision: Revision) -> Path:
        """Add one revision to the stored chain, validating the link."""
        chain = self.load().extend(revision)
        return self.save(chain)[-1]


def _revision_to_dict(revision: Revision) -> dict[str, Any]:
    payload = revision.to_dict(skip_defaults=True)
    # ``ops`` needs the codec's verified serialization, not a plain dump: a
    # nested discriminator with a default would otherwise be dropped.
    payload["ops"] = ops_to_dicts(list(revision.ops))
    payload["revision"] = revision.revision
    return payload


def _revision_from_dict(payload: dict[str, Any]) -> Revision:
    data = dict(payload)
    data["ops"] = ops_from_dicts(list(data.get("ops", [])))
    return Revision.model_validate(data)


def _link_order(revisions: list[Revision]) -> list[Revision]:
    """Order revisions by their parent links rather than by filename."""
    if not revisions:
        return []
    by_parent: dict[str | None, Revision] = {}
    for revision in revisions:
        if revision.down_revision in by_parent:
            raise RevisionError(
                f"two revisions share parent '{revision.down_revision}'; "
                "the chain is linear in this version"
            )
        by_parent[revision.down_revision] = revision

    ordered: list[Revision] = []
    parent: str | None = None
    while parent in by_parent:
        revision = by_parent.pop(parent)
        ordered.append(revision)
        parent = revision.revision
    if by_parent:
        raise RevisionError(
            "stored revisions do not form a single chain; orphaned parents: "
            f"{sorted(str(key) for key in by_parent)}"
        )
    return ordered


__all__ = [
    "REVISION_ID_LENGTH",
    "FileRevisionStore",
    "Revision",
    "RevisionChain",
    "RevisionError",
    "apply_revisions",
    "build_revision",
    "compute_revision_id",
    "downgrade_to",
]
