"""Internal helpers for :class:`~graflo.architecture.evolution.ops.SanitizeOp`.

Hosts the analytic / planning side of sanitization: pure functions that compute
which fields must be renamed and the per-vertex field rewrite map produced by
TigerGraph's "consistent identity per relation" constraint.

The actual mutation lives in
:mod:`graflo.architecture.evolution.apply` (``apply_sanitize`` and
``apply_rename_vertex_properties``) so the same code paths drive both
``SanitizeOp`` and the standalone ``RenameVertexPropertiesOp``.
"""

from __future__ import annotations

import logging
from collections import Counter

from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.onto import DBType

logger = logging.getLogger(__name__)


def _vertex_field_sanitizer(
    schema: Schema,
    reserved_words: set[str],
    *,
    db_flavor: DBType | None = None,
):
    """Return ``(sanitize, should_run)`` for vertex property name sanitization."""
    from graflo.db.util import (
        load_tigergraph_identifier_rules,
        sanitize_attribute_name,
        sanitize_tigergraph_identifier,
    )

    flavor = db_flavor if db_flavor is not None else schema.db_profile.db_flavor
    if flavor == DBType.TIGERGRAPH:
        rules = load_tigergraph_identifier_rules()
        if rules is None:
            if not reserved_words:
                return None, False
            return (
                lambda name: sanitize_attribute_name(name, reserved_words),
                True,
            )
        effective_reserved = reserved_words or set(rules.reserved_words_upper)

        def sanitize(name: str) -> str:
            return sanitize_tigergraph_identifier(
                name,
                effective_reserved,
                rules.forbidden_prefixes,
                rules.invalid_characters,
            )

        return sanitize, True

    if not reserved_words:
        return None, False

    return (lambda name: sanitize_attribute_name(name, reserved_words), True)


def compute_vertex_field_renames(
    schema: Schema,
    reserved_words: set[str],
    *,
    db_flavor: DBType | None = None,
) -> dict[str, dict[str, str]]:
    """Compute per-vertex field rename map for a flavor's reserved-word set.

    Pure: returns ``{vertex_name: {old_field: new_field}}`` without mutating
    ``schema``. Vertices/fields whose names are not reserved are absent from
    the result.

    For TigerGraph, also renames fields whose names contain invalid identifier
    characters or forbidden prefixes.
    """
    sanitize, should_run = _vertex_field_sanitizer(
        schema, reserved_words, db_flavor=db_flavor
    )
    if not should_run or sanitize is None:
        return {}

    renames: dict[str, dict[str, str]] = {}
    for vertex in schema.core_schema.vertex_config.vertices:
        per_vertex: dict[str, str] = {}
        for field in vertex.properties:
            sanitized = sanitize(field.name)
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

        # Walk existing properties and apply per_vertex rename map, preserving
        # types and descriptions. This mirrors _rename_fields_in_schema and
        # correctly handles overlapping old/new field names (e.g. ("a","b") ->
        # ("b","c") where "b" appears in both sets at different positions).
        new_properties: list[Field] = []
        seen_names: set[str] = set()
        for field in vertex.properties:
            new_name = per_vertex.get(field.name, field.name)
            if new_name in seen_names:
                continue
            seen_names.add(new_name)
            if new_name == field.name:
                new_properties.append(field)
            else:
                new_properties.append(field.model_copy(update={"name": new_name}))

        # Add any identity fields from most_popular_index that have no source
        # in the current properties (genuinely new fields — type=None is correct
        # because there is no existing field to inherit a type from).
        for new_field in most_popular_index:
            if new_field not in seen_names:
                new_properties.append(Field(name=new_field, type=None))
                seen_names.add(new_field)

        # Set identity before properties so that the set_identity model
        # validator (triggered by validate_assignment=True on ConfigBaseModel)
        # reads the new identity when it runs for the properties assignment.
        # If properties were set first, the validator would see the old identity
        # and re-add the old identity field names as type=None ghosts.
        vertex.identity = list(most_popular_index)
        vertex.properties = new_properties

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
