"""Planning the provenance aspect: ``SourceRecord``, ``Agent`` and their edges.

A pure planner over the same op vocabulary as every other manifest change.
It needs no spec: which types read their facts from a source record is the one
thing a caller may add, and the composed lift adds the state and observation
scaffolds to that list itself.
"""

from __future__ import annotations

from collections.abc import Iterable

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.vertex import Vertex

from ...ops import AddEdgesOp, AddVerticesOp, ManifestOp
from ..base import LiftError
from . import vocabulary as vocab


def nonconformance(existing: Vertex, aspect: Vertex) -> list[str]:
    """How *existing* fails to serve as the aspect's *aspect* type; empty if it does.

    It serves when it has the same identity and class IRI and every field of
    the aspect's type, with the same type. Further fields, secondary
    identities and its own description are its own.
    """
    problems: list[str] = []
    if list(existing.identity) != list(aspect.identity):
        problems.append(f"identity {existing.identity} is not {aspect.identity}")
    iri = existing.semantics.iri if existing.semantics is not None else None
    expected_iri = aspect.semantics.iri if aspect.semantics is not None else None
    if iri != expected_iri:
        problems.append(f"class IRI {iri!r} is not {expected_iri!r}")
    fields = {field.name: field for field in existing.properties}
    for field in aspect.properties:
        have = fields.get(field.name)
        if have is None:
            problems.append(f"no field {field.name!r}")
        elif (have.type, have.item_type) != (field.type, field.item_type):
            problems.append(f"field {field.name!r} is {have.type}, not {field.type}")
    return problems


def plan_provenance(
    manifest: GraphManifest, *, attach: Iterable[str] = ()
) -> list[ManifestOp]:
    """The ops that make lineage expressible in *manifest*.

    ``SourceRecord -wasAttributedTo-> Agent`` is emitted even when there is
    nothing else to attach: it is what makes the schema *able* to say where a
    fact came from, which is the question the world-model profile asks.

    A ``SourceRecord`` or ``Agent`` the manifest already has is kept when it
    conforms to the aspect's type (:func:`nonconformance`), and an edge it
    already has is not added again, so kits that carry the vocabulary compose
    in any order.

    Args:
        manifest: the manifest to lift, already ``finish_init()``-ed.
        attach: vertex type names that gain a ``wasDerivedFrom`` edge to
            ``SourceRecord``, in this order. The caller vouches that each one
            exists or is added by the same change set.

    Raises:
        LiftError: the manifest has no schema, or has a ``SourceRecord`` or
            ``Agent`` that does not conform.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise LiftError(
            "cannot lift a manifest with no schema block: there are no types to "
            "attach provenance to"
        )
    vertex_config = schema.core_schema.vertex_config
    existing = set(vertex_config.vertex_set)
    minted: list[Vertex] = []
    for aspect in (vocab.source_record_vertex(), vocab.agent_vertex()):
        if aspect.name not in existing:
            minted.append(aspect)
            continue
        problems = nonconformance(vertex_config[aspect.name], aspect)
        if problems:
            raise LiftError(
                f"the manifest already has {aspect.name!r}, and it cannot serve as "
                f"the provenance aspect's type: {'; '.join(problems)}. Rename it "
                "first, or make it conform"
            )
    edge_config = schema.core_schema.edge_config
    edges = [
        edge
        for edge in [
            *(vocab.derived_from_edge(name) for name in attach),
            vocab.attributed_to_edge(),
        ]
        if edge.edge_id not in edge_config
    ]
    ops: list[ManifestOp] = []
    if minted:
        ops.append(AddVerticesOp(vertices=minted))
    if edges:
        ops.append(AddEdgesOp(edges=edges))
    return ops


__all__ = ["plan_provenance"]
