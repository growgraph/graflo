"""Manifest-level provenance, and the stamping that writes it.

:class:`~graflo.architecture.schema.provenance.Provenance` itself lives at L2,
beside the schema metadata that carries it. What lives here is the manifest's
own metadata block -- ``Schema.metadata`` describes the *schema*, and a manifest
is a larger object that may carry no schema at all -- and the stamping helper.

Stamping is a free function on purpose. It is something a **commit point** (the
CLI, a server route) does to an artifact, not something the artifact does to
itself. Keeping it outside the models is what stops provenance leaking into
``apply_evolution``, which must stay pure: applying the same ops twice cannot be
allowed to produce two artifacts that disagree about their own lineage.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.provenance import Provenance


class ManifestMetadata(ConfigBaseModel):
    """Metadata about a manifest as a whole.

    Provenance is the first thing that genuinely belongs at this level: a
    content address covers all three blocks together, so it cannot hang off the
    schema's metadata without lying about what it addresses.
    """

    provenance: Provenance | None = PydanticField(
        default=None,
        description="Content address and lineage of this manifest.",
    )


def stamp_provenance(
    target: Any,
    *,
    content_hash: str,
    canon: str,
    commit: str | None = None,
    parents: list[str] | None = None,
    merge_recipe: str | None = None,
) -> Provenance:
    """Write a :class:`Provenance` block onto *target*, in place.

    Accepts a ``GraphManifest`` (creating ``metadata`` if absent) or anything
    else carrying a metadata block that holds provenance, such as ``Schema``.

    Args:
        target: The artifact to stamp.
        content_hash: Content address of *target*, excluding provenance.
        canon: Canonicalization version that produced *content_hash*.
        commit: Id of the commit producing this state.
        parents: Parent commit ids, in position order.
        merge_recipe: Content hash of the merge recipe, merges only.

    Returns:
        The provenance block that was written.

    Raises:
        TypeError: *target* has no metadata block to stamp.
    """
    fields = getattr(type(target), "model_fields", None)
    if not isinstance(fields, dict) or "metadata" not in fields:
        raise TypeError(
            f"{type(target).__name__} has no metadata block to stamp provenance on"
        )

    provenance = Provenance(
        content_hash=content_hash,
        canon=canon,
        commit=commit,
        parents=list(parents or []),
        merge_recipe=merge_recipe,
    )

    metadata = getattr(target, "metadata", None)
    if metadata is None:
        metadata = ManifestMetadata()
        object.__setattr__(target, "metadata", metadata)
    object.__setattr__(metadata, "provenance", provenance)
    return provenance


def read_provenance(target: Any) -> Provenance | None:
    """The provenance block on *target*, or ``None`` when it carries none."""
    metadata = getattr(target, "metadata", None)
    return getattr(metadata, "provenance", None) if metadata is not None else None


__all__ = ["ManifestMetadata", "Provenance", "read_provenance", "stamp_provenance"]
