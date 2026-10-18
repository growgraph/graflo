"""Planning the provenance aspect: ``SourceRecord``, ``Agent`` and their edges.

A pure planner over the same op vocabulary as every other manifest change.
It needs no spec: which types read their facts from a source record is the one
thing a caller may add, and the composed lift adds the state and observation
scaffolds to that list itself.
"""

from __future__ import annotations

from collections.abc import Iterable

from graflo.architecture.contract.manifest import GraphManifest

from ...ops import AddEdgesOp, AddVerticesOp, ManifestOp
from ..base import LiftError
from . import vocabulary as vocab


def plan_provenance(
    manifest: GraphManifest, *, attach: Iterable[str] = ()
) -> list[ManifestOp]:
    """The ops that make lineage expressible in *manifest*.

    ``SourceRecord -wasAttributedTo-> Agent`` is emitted even when there is
    nothing else to attach: it is what makes the schema *able* to say where a
    fact came from, which is the question the world-model profile asks.

    Args:
        manifest: the manifest to lift, already ``finish_init()``-ed.
        attach: vertex type names that gain a ``wasDerivedFrom`` edge to
            ``SourceRecord``, in this order. The caller vouches that each one
            exists or is added by the same change set.

    Raises:
        LiftError: the manifest has no schema, or already has a type named
            ``SourceRecord`` or ``Agent``.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise LiftError(
            "cannot lift a manifest with no schema block: there are no types to "
            "attach provenance to"
        )
    existing = set(schema.core_schema.vertex_config.vertex_set)
    taken = sorted({vocab.SOURCE_RECORD, vocab.AGENT} & existing)
    if taken:
        raise LiftError(
            f"the lift would mint types the manifest already has: {taken}. Rename "
            "them first, or leave the provenance aspect out if they already serve "
            "this role"
        )
    edges = [vocab.derived_from_edge(name) for name in attach]
    edges.append(vocab.attributed_to_edge())
    return [
        AddVerticesOp(vertices=[vocab.source_record_vertex(), vocab.agent_vertex()]),
        AddEdgesOp(edges=edges),
    ]


__all__ = ["plan_provenance"]
