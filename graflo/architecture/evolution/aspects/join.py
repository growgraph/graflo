"""A lift is a join: the manifest gains what an aspect's vocabulary has and it lacks.

Every aspect plans through :func:`join`. An element of the vocabulary the
manifest lacks is added; one it already has is kept, and gains only the parts
it is missing; one that contradicts it is a conflict, and every conflict is
reported in one :class:`~graflo.architecture.evolution.aspects.base.LiftError`.

What contradicts, and why it is not joined:

- an identity: it decides which records are one entity, so another one re-keys
  stored data;
- an IRI on a type, an edge or a property: another one names another concept;
- a property's type, ``reserved`` or ``merge``: another one changes how stored
  values are read or written;
- an edge's ``directed``.

A property or IRI that is absent is not a contradiction, and neither are the
manifest's own further properties, secondary identities and descriptions.
Because nothing is overwritten, a join is idempotent and does not depend on
the order manifests are composed and lifted in.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypeVar

from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, SuperClass, Vertex

from ..ops import (
    AddEdgePropertiesOp,
    AddEdgesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    ChangeFieldTypesOp,
    EdgeFieldSemanticsTarget,
    EdgeFieldTypes,
    EdgePropertyAdditions,
    EdgeSubpropertyTarget,
    FieldSemanticsTarget,
    FieldTypeSpec,
    ManifestOp,
    SetEdgeSemanticsOp,
    SetEdgeSubpropertyOfOp,
    SetFieldSemanticsOp,
    SetVertexSemanticsOp,
    SetVertexSubclassOfOp,
)
from .base import LiftError

_S = TypeVar("_S", Semantics, FieldSemantics)


def _key(vertex: Vertex) -> tuple[object, ...]:
    """What *vertex* is keyed by, as declared.

    A hash or blank identity is filled in when the schema is finished, so the
    declaration, not the filled-in ``identity``, is what two types compare by.
    """
    mode = vertex.identity_mode
    if mode == "natural":
        return (mode, tuple(vertex.identity))
    if mode == "hash":
        return (mode, tuple(vertex.hash_identity_properties), vertex.identity_funnel)
    return (mode,)


def _describe_key(vertex: Vertex) -> str:
    mode = vertex.identity_mode
    if mode == "natural":
        return str(list(vertex.identity))
    if mode == "hash":
        return f"a hash of {list(vertex.hash_identity_properties)}"
    return f"a {mode} key"


_T = TypeVar("_T")


def _union(have: Sequence[_T], want: Sequence[_T]) -> list[_T]:
    return [*have, *(item for item in want if item not in have)]


def _join_semantics(
    have: _S | None, want: _S | None, where: str, conflicts: list[str]
) -> _S | None:
    """*have* with *want*'s missing parts, or ``None`` when nothing changes."""
    if want is None:
        return None
    if have is None:
        return want
    if have.iri is not None and want.iri is not None and have.iri != want.iri:
        conflicts.append(f"{where} is grounded as {have.iri!r}, not {want.iri!r}")
        return None
    update: dict[str, object] = {
        "iri": have.iri or want.iri,
        "label": have.label or want.label,
        "exact_match": _union(have.exact_match, want.exact_match),
        "synonyms": _union(have.synonyms, want.synonyms),
        "close_match": _union(have.close_match, want.close_match),
        "broader": _union(have.broader, want.broader),
    }
    if isinstance(have, FieldSemantics) and isinstance(want, FieldSemantics):
        if have.unit is not None and want.unit is not None and have.unit != want.unit:
            conflicts.append(f"{where} is in {have.unit!r}, not {want.unit!r}")
            return None
        update["unit"] = have.unit or want.unit
    joined = have.model_copy(update=update)
    return None if joined == have else joined


class _Fields:
    """What joining one element's properties needs: additions, retypes, groundings."""

    def __init__(self) -> None:
        self.added: list[Field] = []
        self.retyped: dict[str, FieldTypeSpec] = {}
        self.grounded: dict[str, FieldSemantics] = {}


def _join_fields(
    have: Sequence[Field], want: Sequence[Field], where: str, conflicts: list[str]
) -> _Fields:
    out = _Fields()
    by_name = {field.name: field for field in have}
    for field in want:
        present = by_name.get(field.name)
        if present is None:
            out.added.append(field)
            continue
        at = f"{where}.{field.name}"
        if present.type is None and field.type is not None:
            out.retyped[field.name] = FieldTypeSpec(
                type=field.type, item_type=field.item_type
            )
        elif field.type is not None and (present.type, present.item_type) != (
            field.type,
            field.item_type,
        ):
            conflicts.append(f"{at} is {present.type}, not {field.type}")
        if present.reserved != field.reserved:
            conflicts.append(
                f"{at} is {'' if present.reserved else 'not '}reserved, "
                f"the aspect's is {'' if field.reserved else 'not '}reserved"
            )
        if present.merge != field.merge:
            conflicts.append(f"{at} merges by {present.merge!r}, not {field.merge!r}")
        for attribute in ("match", "derived"):
            have, wanted = getattr(present, attribute), getattr(field, attribute)
            if wanted is not None and have != wanted:
                conflicts.append(f"{at} declares {attribute} {have}, not {wanted}")
        grounded = _join_semantics(present.semantics, field.semantics, at, conflicts)
        if grounded is not None:
            out.grounded[field.name] = grounded
    return out


def join(
    core: CoreSchema,
    *,
    vertices: Sequence[Vertex] = (),
    vertex_fields: Mapping[str, Sequence[Field]] | None = None,
    edges: Sequence[Edge] = (),
    relation_fields: Mapping[str, Sequence[Field]] | None = None,
) -> list[ManifestOp]:
    """The ops that join *core* with a vocabulary.

    Args:
        core: the manifest's schema, as the earlier aspects of the same lift
            left it.
        vertices: vocabulary types, joined by name.
        vertex_fields: vocabulary properties for types *core* must declare.
        edges: vocabulary edges, joined by ``(source, target, relation)``. A
            new edge's ends must exist in *core* or among *vertices*.
        relation_fields: vocabulary properties for every edge of each
            relation, which *core* must declare.

    Returns:
        Ops in application order; empty when *core* already holds it all.

    Raises:
        LiftError: a type or relation the fields name is missing, or the
            vocabulary contradicts *core*; every contradiction is listed.
    """
    vertex_config = core.vertex_config
    edge_config = core.edge_config
    vertex_fields = vertex_fields or {}
    relation_fields = relation_fields or {}
    declared_relations = {edge.relation for edge in edge_config.values()}
    unknown = sorted(set(vertex_fields) - set(vertex_config.vertex_set)) + sorted(
        set(relation_fields) - declared_relations
    )
    if unknown:
        raise LiftError(
            f"the lift names types or relations the manifest lacks: {unknown}"
        )

    conflicts: list[str] = []
    new_vertices: list[Vertex] = []
    vertex_semantics: dict[str, Semantics] = {}
    added_fields: dict[str, list[Field]] = {}
    retypes: dict[str, dict[str, FieldTypeSpec]] = {}
    groundings: list[FieldSemanticsTarget | EdgeFieldSemanticsTarget] = []
    vertex_parents: dict[str, list[SuperClass]] = {}

    wanted: dict[str, list[Field]] = {}
    for vertex in vertices:
        if vertex.name not in vertex_config.vertex_set:
            new_vertices.append(vertex)
            continue
        present = vertex_config[vertex.name]
        if _key(present) != _key(vertex):
            conflicts.append(
                f"{vertex.name} is keyed by {_describe_key(present)}, "
                f"not {_describe_key(vertex)}"
            )
        grounded = _join_semantics(
            present.semantics, vertex.semantics, vertex.name, conflicts
        )
        if grounded is not None:
            vertex_semantics[vertex.name] = grounded
        parents = _union(present.subclass_of, vertex.subclass_of)
        if parents != present.subclass_of:
            vertex_parents[vertex.name] = parents
        wanted.setdefault(vertex.name, []).extend(vertex.properties)
    for name, fields in vertex_fields.items():
        wanted.setdefault(name, []).extend(fields)
    for name, fields in wanted.items():
        joined = _join_fields(vertex_config[name].properties, fields, name, conflicts)
        if joined.added:
            added_fields[name] = joined.added
        if joined.retyped:
            retypes[name] = joined.retyped
        groundings += [
            FieldSemanticsTarget(vertex=name, field=field, semantics=semantics)
            for field, semantics in joined.grounded.items()
        ]

    new_edges: list[Edge] = []
    edge_semantics: list[SetEdgeSemanticsOp] = []
    edge_parents: list[EdgeSubpropertyTarget] = []
    edge_additions: list[EdgePropertyAdditions] = []
    edge_retypes: list[EdgeFieldTypes] = []
    wanted_on_edges: dict[tuple[str, str, str | None], list[Field]] = {}
    for edge in edges:
        if edge.edge_id not in edge_config:
            new_edges.append(edge)
            continue
        present = edge_config.edge_for(edge.edge_id)
        where = "-".join(str(part) for part in edge.edge_id)
        if present.directed != edge.directed:
            conflicts.append(
                f"{where} is {'' if present.directed else 'un'}directed, the "
                f"aspect's is {'' if edge.directed else 'un'}directed"
            )
        grounded = _join_semantics(present.semantics, edge.semantics, where, conflicts)
        if grounded is not None:
            edge_semantics.append(
                SetEdgeSemanticsOp(
                    edges=[
                        {
                            "source": edge.source,
                            "target": edge.target,
                            "relation": edge.relation,
                        }
                    ],
                    semantics=grounded,
                )
            )
        refs = _union(present.subproperty_of, edge.subproperty_of)
        if refs != present.subproperty_of:
            edge_parents.append(
                EdgeSubpropertyTarget(
                    source=edge.source,
                    target=edge.target,
                    relation=edge.relation,
                    subproperty_of=refs,
                )
            )
        wanted_on_edges.setdefault(edge.edge_id, []).extend(edge.properties)
    for relation, fields in relation_fields.items():
        for present in edge_config.values():
            if present.relation == relation:
                wanted_on_edges.setdefault(present.edge_id, []).extend(fields)
    for edge_id, fields in wanted_on_edges.items():
        source, target, relation = edge_id
        where = "-".join(str(part) for part in edge_id)
        present = edge_config.edge_for(edge_id)
        joined = _join_fields(present.properties, fields, where, conflicts)
        if joined.added:
            edge_additions.append(
                EdgePropertyAdditions(
                    source=source,
                    target=target,
                    relation=relation,
                    properties=list(joined.added),
                )
            )
        if joined.retyped:
            edge_retypes.append(
                EdgeFieldTypes(
                    source=source,
                    target=target,
                    relation=relation,
                    fields=joined.retyped,
                )
            )
        groundings += [
            EdgeFieldSemanticsTarget(
                source=source,
                target=target,
                relation=relation,
                field=field,
                semantics=semantics,
            )
            for field, semantics in joined.grounded.items()
        ]

    if conflicts:
        raise LiftError(
            "the lift contradicts what the manifest declares: "
            + "; ".join(conflicts)
            + ". Rename or change these first"
        )

    ops: list[ManifestOp] = []
    if new_vertices:
        ops.append(AddVerticesOp(vertices=new_vertices))
    if vertex_semantics:
        ops.append(SetVertexSemanticsOp(semantics=dict(vertex_semantics)))
    if vertex_parents:
        ops.append(SetVertexSubclassOfOp(subclass_of=vertex_parents))
    if retypes or edge_retypes:
        ops.append(ChangeFieldTypesOp(vertices=retypes, per_edge=edge_retypes))
    if added_fields:
        ops.append(AddVertexPropertiesOp(additions=dict(added_fields)))
    if new_edges:
        ops.append(AddEdgesOp(edges=new_edges))
    ops += edge_semantics
    if edge_parents:
        ops.append(SetEdgeSubpropertyOfOp(edges=edge_parents))
    if edge_additions:
        ops.append(AddEdgePropertiesOp(per_edge=edge_additions))
    if groundings:
        ops.append(SetFieldSemanticsOp(targets=groundings))
    return ops


__all__ = ["join"]
