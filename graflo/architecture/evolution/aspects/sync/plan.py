"""Planning the sync aspect: a reserved ``sync_hash`` field on every owned type.

The types are the ones a resource's ``sync`` block owns, so the aspect needs no
spec: the manifest already says which records replace which vertices.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.vertex import Field, FieldType

from ...ops import ManifestOp
from ..base import LiftError
from ..join import join

#: The field the lift adds.
SYNC_HASH = "sync_hash"


def sync_field() -> Field:
    """``sync_hash``: a reserved digest of what the owner record wrote."""
    return Field(
        name=SYNC_HASH,
        type=FieldType.STRING,
        reserved=True,
        set_by="sync",
        description=(
            "Digest of what the owner record wrote: the vertex's properties and "
            "its owned edges. Set by a bulk or sync write; null after an upsert."
        ),
    )


def plan_sync(manifest: GraphManifest) -> list[ManifestOp]:
    """The ops that give every type a resource owns a ``sync_hash``.

    Raises:
        LiftError: no schema, or no resource declares a ``sync`` block.
    """
    if manifest.graph_schema is None:
        raise LiftError("cannot lift a manifest with no schema block")
    model = manifest.ingestion_model
    owned = sorted(
        {r.sync.owns for r in (model.resources if model else []) if r.sync is not None}
    )
    if not owned:
        raise LiftError(
            "the sync aspect labels the types a resource's sync block owns; "
            "no resource declares one"
        )
    return join(
        manifest.graph_schema.core_schema,
        vertex_fields={name: [sync_field()] for name in owned},
    )


__all__ = ["SYNC_HASH", "plan_sync", "sync_field"]
