"""Typing and physical-profile evolution ops.

Property types and secondary indexes were visible to the schema differ long before
anything could author them. These ops close that half of the loop; they do not touch
a live database, which stays the job of the migration executor.
"""

from __future__ import annotations

import logging

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import Index
from graflo.architecture.refusal import OpRefusal
from graflo.architecture.schema.vertex import Field, FieldType

from .ops import (
    AddEdgeIndexesOp,
    AddVertexIndexesOp,
    ChangeFieldTypesOp,
    FieldTypeSpec,
    RemoveEdgeIndexesOp,
    RemoveVertexIndexesOp,
    SetBindingsOp,
    SetDbProfileOp,
    SetEdgeDirectedOp,
    SetNativeInversesOp,
)
from .refusal_subjects import edge_subject, edges, names, prop_subject

logger = logging.getLogger(__name__)


def _retyped(field: Field, spec: FieldTypeSpec) -> Field:
    return field.model_copy(update={"type": spec.type, "item_type": spec.item_type})


def _assert_supported(manifest: GraphManifest, field: Field, owner: str) -> None:
    """Reject a target type the profile's backend cannot store natively.

    *owner* is the refusal subject of the vertex or edge that holds *field*.
    """
    from graflo.db.field_type_support import (
        UnsupportedFieldTypeError,
        assert_field_type_supported,
    )

    schema = manifest.graph_schema
    if schema is None:
        return
    try:
        assert_field_type_supported(schema.db_profile.db_flavor, field)
    except UnsupportedFieldTypeError as exc:
        raise OpRefusal(
            str(exc),
            check="not-applicable",
            subjects=(prop_subject(owner, field.name),),
        ) from exc


def apply_change_field_types(manifest: GraphManifest, op: ChangeFieldTypesOp) -> None:
    """Set the logical type of existing vertex and edge properties."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "change_field_types requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.vertices) - vertex_config.vertex_set)
    if unknown:
        raise OpRefusal(
            f"change_field_types: unknown vertices: {unknown}",
            check="unknown-vertex",
            subjects=names(unknown),
        )

    for vertex in vertex_config.vertices:
        changes = op.vertices.get(vertex.name)
        if not changes:
            continue
        declared = {field.name for field in vertex.properties}
        missing = sorted(set(changes) - declared)
        if missing:
            raise OpRefusal(
                f"change_field_types: vertex '{vertex.name}' does not declare {missing}",
                check="unknown-property",
                subjects=names(prop_subject(vertex.name, name) for name in missing),
            )
        identity_fields = set(vertex.identity) | set(vertex.hash_identity_properties)
        for field_name, spec in changes.items():
            if spec.type == FieldType.LIST and field_name in identity_fields:
                raise OpRefusal(
                    f"change_field_types: vertex '{vertex.name}' field "
                    f"'{field_name}' participates in the identity and cannot become "
                    "a LIST",
                    check="identity-field",
                    subjects=(prop_subject(vertex.name, field_name),),
                )
        new_properties = []
        for field in vertex.properties:
            spec = changes.get(field.name)
            if spec is None:
                new_properties.append(field)
                continue
            retyped = _retyped(field, spec)
            _assert_supported(manifest, retyped, vertex.name)
            new_properties.append(retyped)
        vertex.properties = new_properties

    if op.edges or op.per_edge:
        relations = {
            edge.relation
            for edge in schema.core_schema.edge_config.edges
            if edge.relation
        }
        unknown_relations = sorted(set(op.edges) - relations)
        if unknown_relations:
            raise OpRefusal(
                f"change_field_types: unknown relations: {unknown_relations}",
                check="unknown-relation",
                subjects=names(unknown_relations),
            )

        # A relation can join several endpoint pairs, each declaring its own
        # properties: a field is retyped on every edge of the relation that
        # declares it, and refused only when none does.
        edges = schema.core_schema.edge_config.edges
        for relation, changes in op.edges.items():
            declared = {
                field.name
                for edge in edges
                if edge.relation == relation
                for field in edge.properties
            }
            missing = sorted(set(changes) - declared)
            if missing:
                raise OpRefusal(
                    f"change_field_types: edge '{relation}' does not declare {missing}",
                    check="unknown-property",
                    subjects=names(prop_subject(relation, name) for name in missing),
                )
        on_edge = {edge.edge_id: {f.name for f in edge.properties} for edge in edges}
        for entry in op.per_edge:
            if entry.edge_id() not in on_edge:
                raise OpRefusal(
                    f"change_field_types: unknown edge: {entry.edge_id()}",
                    check="unknown-edge",
                    subjects=(edge_subject(*entry.edge_id()),),
                )
            missing = sorted(set(entry.fields) - on_edge[entry.edge_id()])
            if missing:
                raise OpRefusal(
                    f"change_field_types: edge {entry.edge_id()} does not declare "
                    f"{missing}",
                    check="unknown-property",
                    subjects=names(
                        prop_subject(edge_subject(*entry.edge_id()), name)
                        for name in missing
                    ),
                )

        for edge in edges:
            changes = op.edge_changes_for(edge.edge_id)
            if not changes:
                continue
            declared = {field.name for field in edge.properties}
            identity_tokens = {token for key in edge.identities for token in key}
            for field_name, spec in changes.items():
                if field_name not in declared:
                    continue
                if spec.type == FieldType.LIST and field_name in identity_tokens:
                    raise OpRefusal(
                        f"change_field_types: edge '{edge.relation}' field "
                        f"'{field_name}' participates in an identity key and "
                        "cannot become a LIST",
                        check="identity-field",
                        subjects=(
                            prop_subject(edge_subject(*edge.edge_id), field_name),
                        ),
                    )
            new_properties = []
            for field in edge.properties:
                spec = changes.get(field.name)
                if spec is None:
                    new_properties.append(field)
                    continue
                retyped = _retyped(field, spec)
                _assert_supported(manifest, retyped, edge_subject(*edge.edge_id))
                new_properties.append(retyped)
            edge.properties = new_properties

    schema.finish_init()


def _derived_secondary_identity_field_sets(
    manifest: GraphManifest, vertex_name: str
) -> set[frozenset[str]]:
    schema = manifest.graph_schema
    if schema is None:
        return set()
    for vertex in schema.core_schema.vertex_config.vertices:
        if vertex.name == vertex_name:
            return {entry.field_set for entry in vertex.secondary_identities}
    return set()


def apply_add_vertex_indexes(manifest: GraphManifest, op: AddVertexIndexesOp) -> None:
    """Author secondary indexes on vertices."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "add_vertex_indexes requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.indexes) - vertex_config.vertex_set)
    if unknown:
        raise OpRefusal(
            f"add_vertex_indexes: unknown vertices: {unknown}",
            check="unknown-vertex",
            subjects=names(unknown),
        )

    for vertex_name, indexes in op.indexes.items():
        declared = {
            field.name
            for vertex in vertex_config.vertices
            if vertex.name == vertex_name
            for field in vertex.properties
        }
        for index in indexes:
            missing = [name for name in index.fields if name not in declared]
            if missing:
                raise OpRefusal(
                    f"add_vertex_indexes: vertex '{vertex_name}' does not declare "
                    f"{missing}",
                    check="unknown-property",
                    subjects=names(prop_subject(vertex_name, name) for name in missing),
                )
            schema.db_profile.add_vertex_index(vertex_name, index.model_copy(deep=True))

    schema.finish_init()


def apply_remove_vertex_indexes(
    manifest: GraphManifest, op: RemoveVertexIndexesOp
) -> None:
    """Withdraw authored vertex indexes, refusing to touch derived ones."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "remove_vertex_indexes requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    unknown = sorted(set(op.indexes) - schema.core_schema.vertex_config.vertex_set)
    if unknown:
        raise OpRefusal(
            f"remove_vertex_indexes: unknown vertices: {unknown}",
            check="unknown-vertex",
            subjects=names(unknown),
        )

    for vertex_name, field_lists in op.indexes.items():
        derived = _derived_secondary_identity_field_sets(manifest, vertex_name)
        doomed = {frozenset(fields) for fields in field_lists}
        collision = sorted(sorted(entry) for entry in doomed & derived)
        if collision:
            raise OpRefusal(
                f"remove_vertex_indexes: vertex '{vertex_name}' indexes {collision} "
                "are derived from secondary_identities and would be re-registered by "
                "the next finish_init; use RemoveSecondaryIdentitiesOp instead",
                check="still-referenced",
                subjects=(vertex_name,),
            )

        existing = schema.db_profile.vertex_indexes.get(vertex_name, [])
        present = {frozenset(index.fields) for index in existing}
        unmatched = sorted(sorted(entry) for entry in doomed - present)
        if unmatched:
            raise OpRefusal(
                f"remove_vertex_indexes: vertex '{vertex_name}' has no index on "
                f"{unmatched}",
                check="unknown-index",
                subjects=(vertex_name,),
            )
        schema.db_profile.vertex_indexes[vertex_name] = [
            index for index in existing if frozenset(index.fields) not in doomed
        ]

    # finish_init prunes any entry this emptied — an empty list and an absent
    # key mean the same thing but hash differently.
    schema.finish_init()


def _edge_spec_for(
    manifest: GraphManifest, key: tuple[str, str, str | None, str | None]
):
    schema = manifest.graph_schema
    if schema is None:
        return None
    for spec in schema.db_profile.edge_specs:
        if spec.physical_key == key:
            return spec
    return None


def apply_add_edge_indexes(manifest: GraphManifest, op: AddEdgeIndexesOp) -> None:
    """Author secondary indexes on edge physical specs."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "add_edge_indexes requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    known_edges = {edge.edge_id for edge in schema.core_schema.edge_config.edges}
    for entry in op.edges:
        edge_id = (entry.source, entry.target, entry.relation)
        if edge_id not in known_edges:
            raise OpRefusal(
                f"add_edge_indexes: unknown edge: {edge_id}",
                check="unknown-edge",
                subjects=(edge_subject(*edge_id),),
            )
        if not entry.indexes:
            raise OpRefusal(
                f"add_edge_indexes: edge {edge_id} lists no indexes",
                check="invalid-op",
                subjects=(edge_subject(*edge_id),),
            )

        spec = _edge_spec_for(manifest, entry.physical_key())
        if spec is None:
            # The edge is declared (checked above); its profile has no spec
            # for this purpose.
            raise OpRefusal(
                f"add_edge_indexes: no physical spec for {entry.physical_key()}",
                check="not-applicable",
                subjects=(edge_subject(*edge_id),),
            )
        existing = {frozenset(index.fields) for index in spec.indexes}
        additions: list[Index] = []
        for index in entry.indexes:
            if frozenset(index.fields) in existing:
                raise OpRefusal(
                    f"add_edge_indexes: {entry.physical_key()} already indexes "
                    f"{index.fields}",
                    check="already-exists",
                    subjects=(edge_subject(*edge_id),),
                )
            existing.add(frozenset(index.fields))
            additions.append(index.model_copy(deep=True))
        spec.indexes = [*spec.indexes, *additions]

    schema.finish_init()


def apply_remove_edge_indexes(manifest: GraphManifest, op: RemoveEdgeIndexesOp) -> None:
    """Withdraw authored indexes from edge physical specs."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "remove_edge_indexes requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    known_edges = {edge.edge_id for edge in schema.core_schema.edge_config.edges}
    for entry in op.edges:
        edge_id = (entry.source, entry.target, entry.relation)
        if not entry.fields:
            raise OpRefusal(
                f"remove_edge_indexes: edge {entry.physical_key()} lists no fields",
                check="invalid-op",
                subjects=(edge_subject(*edge_id),),
            )
        spec = _edge_spec_for(manifest, entry.physical_key())
        if spec is None and edge_id not in known_edges:
            raise OpRefusal(
                f"remove_edge_indexes: no physical spec for {entry.physical_key()}",
                check="unknown-edge",
                subjects=(edge_subject(*edge_id),),
            )
        if spec is None:
            # A declared edge with no spec for this purpose holds no index to remove.
            raise OpRefusal(
                f"remove_edge_indexes: no physical spec for {entry.physical_key()}",
                check="unknown-index",
                subjects=(edge_subject(*edge_id),),
            )
        doomed = {frozenset(fields) for fields in entry.fields}
        present = {frozenset(index.fields) for index in spec.indexes}
        unmatched = sorted(sorted(item) for item in doomed - present)
        if unmatched:
            raise OpRefusal(
                f"remove_edge_indexes: {entry.physical_key()} has no index on "
                f"{unmatched}",
                check="unknown-index",
                subjects=(edge_subject(*edge_id),),
            )
        spec.indexes = [
            index for index in spec.indexes if frozenset(index.fields) not in doomed
        ]

    schema.finish_init()


def apply_set_edge_directed(manifest: GraphManifest, op: SetEdgeDirectedOp) -> None:
    """Set the ``directed`` flag on selected edges."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_edge_directed requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    by_edge_id = {edge.edge_id: edge for edge in schema.core_schema.edge_config.edges}
    unknown_ids = [
        selector.edge_id()
        for selector in op.edges
        if selector.edge_id() not in by_edge_id
    ]
    unknown = sorted(str(edge_id) for edge_id in unknown_ids)
    if unknown:
        raise OpRefusal(
            f"set_edge_directed: unknown edges: {unknown}",
            check="unknown-edge",
            subjects=edges(unknown_ids),
        )

    for selector in op.edges:
        by_edge_id[selector.edge_id()].directed = op.directed

    schema.finish_init()


def apply_set_native_inverses(manifest: GraphManifest, op: SetNativeInversesOp) -> None:
    """Add relations to, or withdraw them from, ``db_profile.native_inverses``.

    The rules (declared pair, not symmetric, no explicit inverse edges, one
    physical edge type, TigerGraph) are the schema's own and are checked by
    :meth:`Schema.finish_init`; this op only names itself in the refusal.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_native_inverses requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    known = {
        edge.relation
        for edge in schema.core_schema.edge_config.edges
        if edge.relation is not None
    }
    unknown = sorted(set(op.relations) - known)
    if unknown:
        raise OpRefusal(
            f"set_native_inverses: unknown relations: {unknown}",
            check="unknown-relation",
            subjects=names(unknown),
        )

    profile = schema.db_profile
    current = set(profile.native_inverses)
    selected = set(op.relations)
    profile.native_inverses = sorted(
        current | selected if op.enabled else current - selected
    )
    try:
        schema.finish_init()
    except ValueError as exc:
        raise OpRefusal(
            f"set_native_inverses: {exc}",
            check="invalid-result",
            subjects=names(op.relations),
        ) from exc


def apply_set_bindings(manifest: GraphManifest, op: SetBindingsOp) -> None:
    """Replace the manifest's bindings block."""
    manifest.bindings = (
        op.bindings.model_copy(deep=True) if op.bindings is not None else None
    )


def apply_set_db_profile(manifest: GraphManifest, op: SetDbProfileOp) -> None:
    """Replace the schema's database profile."""
    schema = manifest.graph_schema
    if schema is None:
        raise OpRefusal(
            "set_db_profile requires graph_schema",
            check="missing-block",
            subjects=("schema",),
        )

    schema.db_profile = op.profile.model_copy(deep=True)
    schema.finish_init()
