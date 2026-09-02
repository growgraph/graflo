"""Where an artifact came from, carried inside the artifact.

A schema is a world model; the manifest that produced it is its provenance.
This block makes a shipped artifact self-describing outside any registry: it
names its own content address, the canonicalization that produced it, and the
commits it descends from -- the information a git object carries, in the object.

**Provenance is never part of the content hash.** That exclusion is not an
optimization, it is the definition: content identity must be *path independent*,
so two routes reaching the same world model agree that they did. A hash covering
the parents would make every artifact's identity depend on its history, and
dedup ("we already hold this exact schema") could never fire. The role of a
hash-that-covers-ancestry is played by the commit id instead, exactly as in git.

Lives at L2 beside :class:`~graflo.architecture.schema.metadata.GraphMetadata`,
which carries it. The manifest-level block and the stamping helper are at L3 in
``architecture/contract/provenance.py``, since stamping is something a *commit
point* does to an artifact rather than something the artifact does to itself --
which is what keeps it out of the pure ``apply_evolution`` path.
"""

from __future__ import annotations

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel


class Provenance(ConfigBaseModel):
    """Content address and lineage of a shipped schema or manifest."""

    content_hash: str | None = PydanticField(
        default=None,
        description=(
            "SHA-256 over this artifact's canonical payload, excluding this "
            "block. Two artifacts sharing a content_hash are the same world "
            "model, however they were reached."
        ),
    )
    canon: str | None = PydanticField(
        default=None,
        description=(
            "Canonicalization version that produced content_hash (e.g. "
            "'graflo/canon@2'). Hashes compare only within one canon."
        ),
    )
    parents: list[str] = PydanticField(
        default_factory=list,
        description=(
            "Parent commit ids in position order: empty for a root, one for an "
            "ordinary edit, two or more for a merge or compose. Position is "
            "significant -- a merge's ops are materialized against the first "
            "parent, which is what keeps verified replay working unchanged."
        ),
    )
    commit: str | None = PydanticField(
        default=None,
        description="Id of the commit that produced this state.",
    )
    merge_recipe: str | None = PydanticField(
        default=None,
        description=(
            "Content hash of the recorded MergeRecipe, on merge commits only. "
            "The recipe rides with the commit; this is the pointer a re-merge "
            "follows to find it."
        ),
    )

    @property
    def is_merge(self) -> bool:
        """Whether this state was produced by combining two or more lineages."""
        return len(self.parents) > 1


__all__ = ["Provenance"]
