"""Typing and physical-profile evolution ops.

Property types and secondary indexes were visible to the schema differ long before
anything could author them. These ops close that half of the loop; they do not touch
a live database, which stays the job of the migration executor.
"""

from __future__ import annotations

import logging

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import Index
from graflo.architecture.schema.vertex import Field, FieldType

from .ops import (
    AddEdgeIndexesOp,
    AddVertexIndexesOp,
    ChangeFieldTypesOp,
    FieldTypeSpec,
    RemoveEdgeIndexesOp,
    RemoveVertexIndexesOp,
    SetEdgeDirectedOp,
)

logger = logging.getLogger(__name__)


def _retyped(field: Field, spec: FieldTypeSpec) -> Field:
    return field.model_copy(update={"type": spec.type, "item_type": spec.item_type})


def _assert_supported(manifest: GraphManifest, field: Field) -> None:
    """Reject a target type the profile's backend cannot store natively."""
    from graflo.db.field_type_support import assert_field_type_supported

    schema = manifest.graph_schema
    if schema is None:
        return
    assert_field_type_supported(schema.db_profile.db_flavor, field)


def apply_change_field_types(manifest: GraphManifest, op: ChangeFieldTypesOp) -> None:
    """Set the logical type of existing vertex and edge properties."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("change_field_types requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.vertices) - vertex_config.vertex_set)
    if unknown:
        raise ValueError(f"change_field_types: unknown vertices: {unknown}")

    for vertex in vertex_config.vertices:
        changes = op.vertices.get(vertex.name)
        if not changes:
            continue
        declared = {field.name for field in vertex.properties}
        missing = sorted(set(changes) - declared)
        if missing:
            raise ValueError(
                f"change_field_types: vertex '{vertex.name}' does not declare {missing}"
            )
        identity_fields = set(vertex.identity) | set(vertex.hash_identity_properties)
        for field_name, spec in changes.items():
            if spec.type == FieldType.LIST and field_name in identity_fields:
                raise ValueError(
                    f"change_field_types: vertex '{vertex.name}' field "
                    f"'{field_name}' participates in the identity and cannot become "
                    "a LIST"
                )
        new_properties = []
        for field in vertex.properties:
            spec = changes.get(field.name)
            if spec is None:
                new_properties.append(field)
                continue
            retyped = _retyped(field, spec)
            _assert_supported(manifest, retyped)
            new_properties.append(retyped)
        vertex.properties = new_properties

    if op.edges:
        relations = {
            edge.relation
            for edge in schema.core_schema.edge_config.edges
            if edge.relation
        }
        unknown_relations = sorted(set(op.edges) - relations)
        if unknown_relations:
            raise ValueError(
                f"change_field_types: unknown relations: {unknown_relations}"
            )

        for edge in schema.core_schema.edge_config.edges:
            changes = op.edges.get(edge.relation) if edge.relation else None
            if not changes:
                continue
            declared = {field.name for field in edge.properties}
            missing = sorted(set(changes) - declared)
            if missing:
                raise ValueError(
                    f"change_field_types: edge '{edge.relation}' does not declare "
                    f"{missing}"
                )
            new_properties = []
            for field in edge.properties:
                spec = changes.get(field.name)
                if spec is None:
                    new_properties.append(field)
                    continue
                retyped = _retyped(field, spec)
                _assert_supported(manifest, retyped)
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
        raise ValueError("add_vertex_indexes requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.indexes) - vertex_config.vertex_set)
    if unknown:
        raise ValueError(f"add_vertex_indexes: unknown vertices: {unknown}")

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
                raise ValueError(
                    f"add_vertex_indexes: vertex '{vertex_name}' does not declare "
                    f"{missing}"
                )
            schema.db_profile.add_vertex_index(vertex_name, index.model_copy(deep=True))

    schema.finish_init()


def apply_remove_vertex_indexes(
    manifest: GraphManifest, op: RemoveVertexIndexesOp
) -> None:
    """Withdraw authored vertex indexes, refusing to touch derived ones."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_vertex_indexes requires graph_schema")

    unknown = sorted(set(op.indexes) - schema.core_schema.vertex_config.vertex_set)
    if unknown:
        raise ValueError(f"remove_vertex_indexes: unknown vertices: {unknown}")

    for vertex_name, field_lists in op.indexes.items():
        derived = _derived_secondary_identity_field_sets(manifest, vertex_name)
        doomed = {frozenset(fields) for fields in field_lists}
        collision = sorted(sorted(entry) for entry in doomed & derived)
        if collision:
            raise ValueError(
                f"remove_vertex_indexes: vertex '{vertex_name}' indexes {collision} "
                "are derived from secondary_identities and would be re-registered by "
                "the next finish_init; use RemoveSecondaryIdentitiesOp instead"
            )

        existing = schema.db_profile.vertex_indexes.get(vertex_name, [])
        present = {frozenset(index.fields) for index in existing}
        unmatched = sorted(sorted(entry) for entry in doomed - present)
        if unmatched:
            raise ValueError(
                f"remove_vertex_indexes: vertex '{vertex_name}' has no index on "
                f"{unmatched}"
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
        raise ValueError("add_edge_indexes requires graph_schema")

    known_edges = {edge.edge_id for edge in schema.core_schema.edge_config.edges}
    for entry in op.edges:
        edge_id = (entry.source, entry.target, entry.relation)
        if edge_id not in known_edges:
            raise ValueError(f"add_edge_indexes: unknown edge: {edge_id}")
        if not entry.indexes:
            raise ValueError(f"add_edge_indexes: edge {edge_id} lists no indexes")

        spec = _edge_spec_for(manifest, entry.physical_key())
        if spec is None:
            raise ValueError(
                f"add_edge_indexes: no physical spec for {entry.physical_key()}"
            )
        existing = {frozenset(index.fields) for index in spec.indexes}
        additions: list[Index] = []
        for index in entry.indexes:
            if frozenset(index.fields) in existing:
                raise ValueError(
                    f"add_edge_indexes: {entry.physical_key()} already indexes "
                    f"{index.fields}"
                )
            existing.add(frozenset(index.fields))
            additions.append(index.model_copy(deep=True))
        spec.indexes = [*spec.indexes, *additions]

    schema.finish_init()


def apply_remove_edge_indexes(manifest: GraphManifest, op: RemoveEdgeIndexesOp) -> None:
    """Withdraw authored indexes from edge physical specs."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_edge_indexes requires graph_schema")

    for entry in op.edges:
        if not entry.fields:
            raise ValueError(
                f"remove_edge_indexes: edge {entry.physical_key()} lists no fields"
            )
        spec = _edge_spec_for(manifest, entry.physical_key())
        if spec is None:
            raise ValueError(
                f"remove_edge_indexes: no physical spec for {entry.physical_key()}"
            )
        doomed = {frozenset(fields) for fields in entry.fields}
        present = {frozenset(index.fields) for index in spec.indexes}
        unmatched = sorted(sorted(item) for item in doomed - present)
        if unmatched:
            raise ValueError(
                f"remove_edge_indexes: {entry.physical_key()} has no index on "
                f"{unmatched}"
            )
        spec.indexes = [
            index for index in spec.indexes if frozenset(index.fields) not in doomed
        ]

    schema.finish_init()


def apply_set_edge_directed(manifest: GraphManifest, op: SetEdgeDirectedOp) -> None:
    """Set the ``directed`` flag on selected edges."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("set_edge_directed requires graph_schema")

    by_edge_id = {edge.edge_id: edge for edge in schema.core_schema.edge_config.edges}
    unknown = sorted(
        str(selector.edge_id())
        for selector in op.edges
        if selector.edge_id() not in by_edge_id
    )
    if unknown:
        raise ValueError(f"set_edge_directed: unknown edges: {unknown}")

    for selector in op.edges:
        by_edge_id[selector.edge_id()].directed = op.directed

    schema.finish_init()
