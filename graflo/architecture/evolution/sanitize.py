"""Internal helpers for :class:`~graflo.architecture.evolution.ops.SanitizeOp`.

Hosts the analytic / planning side of sanitization: pure functions that compute
which fields must be renamed and the per-vertex field rewrite map produced by
TigerGraph's "consistent identity per relation" constraint.

The actual mutation lives in
:mod:`graflo.architecture.evolution.apply` (``apply_sanitize`` and
``apply_rename_vertex_fields``) so the same code paths drive both
``SanitizeOp`` and the standalone ``RenameVertexFieldsOp``.
"""

from __future__ import annotations

import logging
from collections import Counter

from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.db.util import sanitize_attribute_name
from graflo.onto import DBType

logger = logging.getLogger(__name__)


def compute_vertex_field_renames(
    schema: Schema,
    reserved_words: set[str],
) -> dict[str, dict[str, str]]:
    """Compute per-vertex field rename map for a flavor's reserved-word set.

    Pure: returns ``{vertex_name: {old_field: new_field}}`` without mutating
    ``schema``. Vertices/fields whose names are not reserved are absent from
    the result.
    """
    renames: dict[str, dict[str, str]] = {}
    if not reserved_words:
        return renames

    for vertex in schema.core_schema.vertex_config.vertices:
        per_vertex: dict[str, str] = {}
        for field in vertex.properties:
            sanitized = sanitize_attribute_name(field.name, reserved_words)
            if sanitized != field.name:
                per_vertex[field.name] = sanitized
        if per_vertex:
            renames[vertex.name] = per_vertex
    return renames


def _normalize_role_indexes(
    vertex_indexes: list[tuple[str, tuple[str, ...]]],
    schema: Schema,
    renames: dict[str, dict[str, str]],
    relation: str | None,
    role: str,
) -> None:
    """Update ``renames`` and ``schema`` so vertices share a common index for *role*.

    Mirrors the legacy ``Sanitizer._normalize_vertex_indexes`` semantics:

    1. Pick the most popular identity tuple as the canonical one.
    2. For each vertex whose identity differs, derive an old->new field map
       and merge it into ``renames[vertex_name]``.
    3. Update the in-memory schema to reflect the new identity (rewrite
       ``vertex.identity`` and ``vertex.properties``).
    """
    from graflo.architecture.schema.vertex import Field

    if not vertex_indexes:
        return

    vertex_index_dict: dict[str, tuple[str, ...]] = {}
    for vertex_name, index_fields in vertex_indexes:
        if vertex_name not in vertex_index_dict:
            vertex_index_dict[vertex_name] = index_fields

    indexes_list = list(vertex_index_dict.values())
    if len(set(indexes_list)) == 1:
        return

    most_popular_index = Counter(indexes_list).most_common(1)[0][0]

    for vertex_name, index_fields in vertex_index_dict.items():
        if index_fields == most_popular_index:
            continue

        old_fields = list(index_fields)
        new_fields = list(most_popular_index)

        per_vertex = renames.setdefault(vertex_name, {})
        if len(old_fields) == len(new_fields):
            for old_field, new_field in zip(old_fields, new_fields):
                if old_field != new_field:
                    per_vertex[old_field] = new_field
        elif old_fields and new_fields and old_fields[0] != new_fields[0]:
            per_vertex[old_fields[0]] = new_fields[0]

        vertex = schema.core_schema.vertex_config[vertex_name]
        existing_field_names = {f.name for f in vertex.properties}
        for new_field in most_popular_index:
            if new_field not in existing_field_names:
                vertex.properties.append(Field(name=new_field, type=None))
                existing_field_names.add(new_field)

        fields_to_remove = [
            f
            for f in vertex.properties
            if f.name in old_fields and f.name not in new_fields
        ]
        for field_to_remove in fields_to_remove:
            vertex.properties.remove(field_to_remove)

        vertex.identity = list(most_popular_index)

        logger.debug(
            "Normalizing %s index for vertex '%s' in relation '%s': %s -> %s",
            role,
            vertex_name,
            relation,
            old_fields,
            new_fields,
        )


def normalize_relation_identity(
    schema: Schema,
    db_flavor: DBType,
) -> dict[str, dict[str, str]]:
    """For TigerGraph: align identity fields across edges sharing a relation.

    Returns a per-vertex ``{old_field: new_field}`` map describing the schema-
    side changes performed. Caller is responsible for propagating the same
    map to ingestion via ``rewrite_vertex_field_names_in_pipeline``.

    For non-TigerGraph flavors this is a no-op and returns an empty dict.
    """
    field_renames: dict[str, dict[str, str]] = {}
    if db_flavor != DBType.TIGERGRAPH:
        return field_renames

    edges_by_relation: dict[str | None, list[Edge]] = {}
    for edge in schema.core_schema.edge_config.edges:
        relation = (
            schema.db_profile.edge_relation_name(
                edge.edge_id,
                default_relation=edge.relation,
            )
            or edge.relation
        )
        edges_by_relation.setdefault(relation, []).append(edge)

    for relation, relation_edges in edges_by_relation.items():
        if len(relation_edges) <= 1:
            continue

        source_indexes: list[tuple[str, tuple[str, ...]]] = []
        target_indexes: list[tuple[str, tuple[str, ...]]] = []
        for edge in relation_edges:
            source_indexes.append(
                (
                    edge.source,
                    tuple(
                        schema.core_schema.vertex_config.identity_fields(edge.source)
                    ),
                )
            )
            target_indexes.append(
                (
                    edge.target,
                    tuple(
                        schema.core_schema.vertex_config.identity_fields(edge.target)
                    ),
                )
            )

        _normalize_role_indexes(
            source_indexes,
            schema,
            field_renames,
            relation,
            role="source",
        )
        _normalize_role_indexes(
            target_indexes,
            schema,
            field_renames,
            relation,
            role="target",
        )

    return field_renames
