"""Dialect-neutral classification of a relational schema into a graph.

Everything here derives from :class:`~graflo.db.sql.provider.SqlMetadataProvider`
and nothing else, which is the point: the heuristics that decide whether a table
is an entity or a relationship are about *shape* — keys, foreign keys, names —
and shape does not vary by engine. Only reading the catalogue does.

Lifted verbatim out of ``PostgresConnection``, which still exposes these as
methods and now delegates here.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import Any

from graflo.architecture.onto_sql import (
    ColumnInfo,
    EdgeTableInfo,
    ForeignKeyInfo,
    RawTableInfo,
    ReferenceEdgeInfo,
    SchemaIntrospectionResult,
    SkippedTableInfo,
    VertexTableInfo,
)
from graflo.db.sql.inference_utils import (
    infer_edge_vertices_from_table_name,
    infer_vertex_from_column_name,
)
from graflo.db.sql.provider import SqlMetadataProvider

logger = logging.getLogger(__name__)


def is_edge_like_table(
    table_name: str, pk_columns: list[str], fk_columns: list[dict[str, Any]]
) -> bool:
    """Determine if a table is edge-like based on heuristics.

    Heuristics:
    1. Tables with 2 or more primary keys are likely edge tables
    2. Tables with exactly 2 foreign keys are likely edge tables
    3. Tables with names starting with 'rel_' are likely edge tables
    4. Tables where primary key columns match foreign key columns are likely edge tables

    Args:
        table_name: Name of the table
        pk_columns: List of primary key column names
        fk_columns: List of foreign key dictionaries

    Returns:
        True if table appears to be edge-like, False otherwise
    """
    # Heuristic 1: Tables with 2 or more primary keys are likely edge tables
    if len(pk_columns) >= 2:
        return True

    # Heuristic 2: Tables with exactly 2 foreign keys are likely edge tables
    if len(fk_columns) == 2:
        return True

    # Heuristic 3: Tables with names starting with 'rel_' are likely edge tables
    if table_name.startswith("rel_"):
        return True

    # Heuristic 4: If primary key columns match foreign key columns, it's likely an edge table
    fk_column_names = {fk["column"] for fk in fk_columns}
    pk_set = set(pk_columns)
    # If all PK columns are FK columns and we have at least 2 FKs, it's likely an edge table
    return bool(pk_set.issubset(fk_column_names) and len(fk_columns) >= 2)


def foreign_keys_in_column_order(
    fk_columns: list[dict[str, Any]], all_columns: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Order foreign-key rows by where their constraint sits in the table.

    An edge table reads *source, then target* in the order its columns were
    declared, and the first foreign key becomes the edge source. No catalog
    promises that order: a query sorted by position *within* a constraint ties
    across single-column constraints, and one sorted by constraint name puts
    ``product_id`` before ``user_id``. The table's own column order is the one
    thing every provider reports the same way.

    A constraint is placed by its earliest column; the rows of a composite key
    keep the order the provider gave them. Rows naming a column the table does
    not list sort last, in their original order.
    """
    position = {col["name"]: index for index, col in enumerate(all_columns)}
    last = len(position)

    def constraint_of(index: int) -> object:
        return fk_columns[index].get("constraint_name") or index

    earliest: dict[object, int] = {}
    first_row: dict[object, int] = {}
    for index, fk in enumerate(fk_columns):
        key = constraint_of(index)
        at = position.get(fk["column"], last)
        earliest[key] = min(earliest.get(key, at), at)
        first_row.setdefault(key, index)

    def rank(index: int) -> tuple[int, int, int]:
        key = constraint_of(index)
        return (earliest[key], first_row[key], index)

    return [fk_columns[index] for index in sorted(range(len(fk_columns)), key=rank)]


def detect_vertex_tables(
    provider: SqlMetadataProvider,
    schema_name: str | None = None,
    *,
    default_schema: str | None = None,
    entity_tables: Collection[str] = (),
) -> list[VertexTableInfo]:
    """Detect vertex-like tables in the schema.

    Heuristic: Tables with a primary key and descriptive columns
    (not just foreign keys). These represent entities.

    Note: Tables identified as edge-like are excluded from vertex tables.

    Args:
        schema_name: Schema name. If None, uses 'public' or config schema_name.
        entity_tables: Tables that are entities whatever their shape. A table
            with two foreign keys looks like a relationship; naming it here
            makes it a vertex and its foreign keys references.

    Returns:
        List of vertex table information dictionaries
    """
    if schema_name is None:
        schema_name = default_schema

    tables = provider.get_tables(schema_name)
    vertex_tables = []

    for table_info in tables:
        table_name = table_info["table_name"]
        pk_columns = provider.get_primary_keys(table_name, schema_name)
        all_columns = provider.get_table_columns(table_name, schema_name)
        fk_columns = foreign_keys_in_column_order(
            provider.get_foreign_keys(table_name, schema_name), all_columns
        )

        # Vertex-like tables have:
        # 1. A primary key
        # 2. Not identified as edge-like tables
        # 3. Descriptive columns beyond just foreign keys

        if not pk_columns:
            continue  # Skip tables without primary keys

        named_entity = table_name in entity_tables

        # Skip edge-like tables
        if not named_entity and is_edge_like_table(table_name, pk_columns, fk_columns):
            continue

        # Count non-FK, non-PK columns (descriptive columns)
        fk_column_names = {fk["column"] for fk in fk_columns}
        pk_column_names = set(pk_columns)
        descriptive_columns = [
            col
            for col in all_columns
            if col["name"] not in fk_column_names and col["name"] not in pk_column_names
        ]

        # If table has descriptive columns, consider it vertex-like
        if descriptive_columns or named_entity:
            # Mark primary key and unique columns and convert to ColumnInfo
            pk_set = set(pk_columns)
            unique_columns = provider.get_unique_columns(table_name, schema_name)
            unique_set = set(unique_columns)
            column_infos = []
            for col in all_columns:
                column_infos.append(
                    ColumnInfo(
                        name=col["name"],
                        type=col["type"],
                        description=col.get("description", ""),
                        is_nullable=col.get("is_nullable", "YES"),
                        column_default=col.get("column_default"),
                        is_pk=col["name"] in pk_set,
                        is_unique=col["name"] in unique_set,
                        ordinal_position=col.get("ordinal_position"),
                    )
                )

            # Convert foreign keys to ForeignKeyInfo
            fk_infos = []
            for fk in fk_columns:
                fk_infos.append(
                    ForeignKeyInfo(
                        column=fk["column"],
                        references_table=fk["references_table"],
                        references_column=fk.get("references_column"),
                        constraint_name=fk.get("constraint_name"),
                    )
                )

            vertex_tables.append(
                VertexTableInfo(
                    name=table_name,
                    schema_name=schema_name or "",
                    columns=column_infos,
                    primary_key=pk_columns,
                    foreign_keys=fk_infos,
                )
            )

    return vertex_tables


def detect_edge_tables(
    provider: SqlMetadataProvider,
    schema_name: str | None = None,
    vertex_table_names: list[str] | None = None,
    *,
    default_schema: str | None = None,
    entity_tables: Collection[str] = (),
) -> list[EdgeTableInfo]:
    """Detect edge-like tables in the schema.

    Heuristic: Tables with 2 or more primary keys, or exactly 2 foreign keys,
    or names starting with 'rel_'. These represent relationships between entities.

    Args:
        schema_name: Schema name. If None, uses 'public' or config schema_name.
        vertex_table_names: Optional list of vertex table names for fuzzy matching.
                          If None, will be inferred from detect_vertex_tables().
        entity_tables: Tables that are entities whatever their shape; never edges.

    Returns:
        List of edge table information dictionaries with source_table and target_table
    """
    if schema_name is None:
        schema_name = default_schema

    # Get vertex table names if not provided
    if vertex_table_names is None:
        vertex_tables = detect_vertex_tables(
            provider,
            schema_name,
            default_schema=default_schema,
            entity_tables=entity_tables,
        )
        vertex_table_names = [vt.name for vt in vertex_tables]

    # Create fuzzy matcher once for all tables (significant performance improvement)
    # Caching is enabled by default for better performance
    from graflo.util.fuzzy_matcher import FuzzyMatcher

    matcher = FuzzyMatcher(vertex_table_names, threshold=0.6, enable_cache=True)

    tables = provider.get_tables(schema_name)
    edge_tables = []

    for table_info in tables:
        table_name = table_info["table_name"]
        pk_columns = provider.get_primary_keys(table_name, schema_name)
        fk_columns = provider.get_foreign_keys(table_name, schema_name)

        # Skip tables without primary keys
        if not pk_columns:
            continue

        # Check if table is edge-like
        if table_name in entity_tables or not is_edge_like_table(
            table_name, pk_columns, fk_columns
        ):
            continue

        all_columns = provider.get_table_columns(table_name, schema_name)
        # The first foreign key is the edge source: read them as declared.
        fk_columns = foreign_keys_in_column_order(fk_columns, all_columns)

        # Mark primary key and unique columns and convert to ColumnInfo
        pk_set = set(pk_columns)
        unique_columns = provider.get_unique_columns(table_name, schema_name)
        unique_set = set(unique_columns)
        column_infos = []
        for col in all_columns:
            column_infos.append(
                ColumnInfo(
                    name=col["name"],
                    type=col["type"],
                    description=col.get("description", ""),
                    is_nullable=col.get("is_nullable", "YES"),
                    column_default=col.get("column_default"),
                    is_pk=col["name"] in pk_set,
                    is_unique=col["name"] in unique_set,
                    ordinal_position=col.get("ordinal_position"),
                )
            )

        # Convert foreign keys to ForeignKeyInfo
        fk_infos = []
        for fk in fk_columns:
            fk_infos.append(
                ForeignKeyInfo(
                    column=fk["column"],
                    references_table=fk["references_table"],
                    references_column=fk.get("references_column"),
                    constraint_name=fk.get("constraint_name"),
                )
            )

        # Determine source and target tables
        source_table = None
        target_table = None
        source_column = None
        target_column = None
        relation_name = None

        # If we have exactly 2 foreign keys, use them directly
        if len(fk_infos) == 2:
            source_fk = fk_infos[0]
            target_fk = fk_infos[1]
            source_table = source_fk.references_table
            target_table = target_fk.references_table
            source_column = source_fk.column
            target_column = target_fk.column
            # Still try to infer relation from table name
            fk_dicts = [
                {
                    "column": fk.column,
                    "references_table": fk.references_table,
                }
                for fk in fk_infos
            ]
            _, _, relation_name = infer_edge_vertices_from_table_name(
                table_name, pk_columns, fk_dicts, vertex_table_names, matcher
            )
        # If we have 2 or more primary keys, try to infer from table name and structure
        elif len(pk_columns) >= 2:
            # Convert fk_infos to dicts for _infer_edge_vertices_from_table_name
            fk_dicts = [
                {
                    "column": fk.column,
                    "references_table": fk.references_table,
                }
                for fk in fk_infos
            ]

            # Try to infer from table name pattern
            inferred_source, inferred_target, relation_name = (
                infer_edge_vertices_from_table_name(
                    table_name,
                    pk_columns,
                    fk_dicts,
                    vertex_table_names,
                    matcher,
                )
            )

            if inferred_source and inferred_target:
                source_table = inferred_source
                target_table = inferred_target
                # Try to match PK columns to FK columns for source/target columns
                if fk_infos:
                    # Use first FK for source, second for target if available
                    if len(fk_infos) >= 2:
                        source_column = fk_infos[0].column
                        target_column = fk_infos[1].column
                    elif len(fk_infos) == 1:
                        # Self-reference case
                        source_column = fk_infos[0].column
                        target_column = fk_infos[0].column
                else:
                    # Use PK columns as source/target columns
                    source_column = pk_columns[0]
                    target_column = (
                        pk_columns[1] if len(pk_columns) > 1 else pk_columns[0]
                    )
            elif fk_infos:
                # Fallback: use FK references if available
                if len(fk_infos) >= 2:
                    source_table = fk_infos[0].references_table
                    target_table = fk_infos[1].references_table
                    source_column = fk_infos[0].column
                    target_column = fk_infos[1].column
                elif len(fk_infos) == 1:
                    source_table = fk_infos[0].references_table
                    target_table = fk_infos[0].references_table
                    source_column = fk_infos[0].column
                    target_column = fk_infos[0].column
            else:
                # Last resort: use PK columns and infer table names from column names
                source_column = pk_columns[0]
                target_column = pk_columns[1] if len(pk_columns) > 1 else pk_columns[0]
                # Use robust inference logic to extract vertex names from column names
                source_table = infer_vertex_from_column_name(
                    source_column, vertex_table_names, matcher
                )
                target_table = infer_vertex_from_column_name(
                    target_column, vertex_table_names, matcher
                )

        # Only add if we have source and target information
        if source_table and target_table:
            edge_tables.append(
                EdgeTableInfo(
                    name=table_name,
                    schema_name=schema_name or "",
                    columns=column_infos,
                    primary_key=pk_columns,
                    foreign_keys=fk_infos,
                    source_table=source_table,
                    target_table=target_table,
                    source_column=source_column or pk_columns[0],
                    target_column=target_column
                    or (pk_columns[1] if len(pk_columns) > 1 else pk_columns[0]),
                    relation=relation_name,
                )
            )
        else:
            logger.debug(
                f"Could not determine source/target tables for edge-like table '{table_name}'. "
                f"Skipping."
            )

    return edge_tables


def _constraints(foreign_keys: list[ForeignKeyInfo]) -> list[list[ForeignKeyInfo]]:
    """Foreign-key rows grouped by constraint; an unnamed row is its own."""
    groups: dict[object, list[ForeignKeyInfo]] = {}
    for index, fk in enumerate(foreign_keys):
        groups.setdefault(fk.constraint_name or index, []).append(fk)
    return list(groups.values())


def _key_columns(
    constraint: list[ForeignKeyInfo], primary_key: list[str]
) -> dict[str, str] | None:
    """``{key column of the target: referring column}``, or None.

    None when the constraint refers to something other than the target's whole
    primary key: a row is matched on its key, and nothing else finds it.
    """
    if len(constraint) == 1 and constraint[0].references_column is None:
        referred = list(primary_key) if len(primary_key) == 1 else []
    else:
        referred = [fk.references_column or "" for fk in constraint]
    if not referred or sorted(referred) != sorted(primary_key):
        return None
    by_key = dict(zip(referred, (fk.column for fk in constraint)))
    return {key: by_key[key] for key in primary_key}


def infer_reference_edges(
    vertex_tables: list[VertexTableInfo],
) -> list[ReferenceEdgeInfo]:
    """Edges stated by the foreign keys of entity tables.

    A foreign key from one entity table to the primary key of another says that
    a row refers to a row: an edge from the referring table to the one referred
    to. The relation is the referring column without a trailing ``_id``, or the
    target table's name for a key of several columns.
    """
    by_name = {table.name: table for table in vertex_tables}
    edges: list[ReferenceEdgeInfo] = []
    for table in vertex_tables:
        relations: set[str] = set()
        for constraint in _constraints(table.foreign_keys):
            first = constraint[0]
            target = by_name.get(first.references_table)
            if target is None:
                continue
            columns = _key_columns(constraint, target.primary_key)
            if columns is None:
                logger.info(
                    "Foreign key %s.%s -> %s is not read as an edge: it does not "
                    "refer to the primary key of '%s'",
                    table.name,
                    first.column,
                    target.name,
                    target.name,
                )
                continue
            relation = target.name
            if len(constraint) == 1:
                relation = first.column.removesuffix("_id") or target.name
            if relation in relations:
                relation = "_".join(fk.column for fk in constraint)
            relations.add(relation)
            edges.append(
                ReferenceEdgeInfo(
                    source_table=table.name,
                    target_table=target.name,
                    relation=relation,
                    columns=columns,
                )
            )
    return edges


def _skipped_tables(
    provider: SqlMetadataProvider, schema_name: str | None, kept: set[str]
) -> list[SkippedTableInfo]:
    """Tables that became neither a vertex nor an edge, each with the reason."""
    skipped: list[SkippedTableInfo] = []
    for table_info in provider.get_tables(schema_name):
        table_name = table_info["table_name"]
        if table_name in kept:
            continue
        pk_columns = provider.get_primary_keys(table_name, schema_name)
        if not pk_columns:
            reason = "no primary key"
        elif is_edge_like_table(
            table_name, pk_columns, provider.get_foreign_keys(table_name, schema_name)
        ):
            reason = "looks like a relationship, but its two ends could not be told"
        else:
            reason = "no columns besides its keys"
        skipped.append(SkippedTableInfo(name=table_name, reason=reason))
    return skipped


def build_raw_tables(
    provider: SqlMetadataProvider, schema_name: str | None = None
) -> list[RawTableInfo]:
    """Build raw table metadata for all tables in the schema."""
    tables = provider.get_tables(schema_name)
    raw_tables = []
    for table_info in tables:
        table_name = table_info["table_name"]
        pk_columns = provider.get_primary_keys(table_name, schema_name)
        fk_columns = provider.get_foreign_keys(table_name, schema_name)
        unique_columns = provider.get_unique_columns(table_name, schema_name)
        all_columns = provider.get_table_columns(table_name, schema_name)
        row_count_estimate = provider.get_table_row_count_estimate(
            table_name, schema_name
        )
        sample_rows = provider.get_table_sample_rows(table_name, schema_name, limit=5)

        pk_set = set(pk_columns)
        unique_set = set(unique_columns)
        # Per-column sample values: list of values from first 5 rows (stringified)
        column_names = [c["name"] for c in all_columns]
        sample_by_col: dict[str, list[str]] = {c: [] for c in column_names}
        for row in sample_rows:
            for col_name in column_names:
                if col_name in row and len(sample_by_col[col_name]) < 5:
                    v = row[col_name]
                    sample_by_col[col_name].append("NULL" if v is None else str(v))

        column_infos = []
        for col in all_columns:
            column_infos.append(
                ColumnInfo(
                    name=col["name"],
                    type=col["type"],
                    description=col.get("description", ""),
                    is_nullable=col.get("is_nullable", "YES"),
                    column_default=col.get("column_default"),
                    is_pk=col["name"] in pk_set,
                    is_unique=col["name"] in unique_set,
                    ordinal_position=col.get("ordinal_position"),
                    sample_values=sample_by_col.get(col["name"], [])[:5],
                )
            )

        fk_infos = [
            ForeignKeyInfo(
                column=fk["column"],
                references_table=fk["references_table"],
                references_column=fk.get("references_column"),
                constraint_name=fk.get("constraint_name"),
            )
            for fk in fk_columns
        ]

        raw_tables.append(
            RawTableInfo(
                name=table_name,
                schema_name=schema_name or "",
                description=table_info.get("description") or "",
                columns=column_infos,
                primary_key=pk_columns,
                foreign_keys=fk_infos,
                row_count_estimate=row_count_estimate,
            )
        )
    return raw_tables


def introspect_schema(
    provider: SqlMetadataProvider,
    schema_name: str | None = None,
    include_raw_tables: bool = False,
    *,
    default_schema: str | None = None,
    entity_tables: Collection[str] | None = None,
) -> SchemaIntrospectionResult:
    """Introspect the database schema and return structured information.

    This is the main method that analyzes the schema and returns information
    about vertex-like and edge-like tables.

    Args:
        schema_name: Schema name. If None, uses 'public' or config schema_name.
        entity_tables: Tables that are entities whatever their shape. Whether a
            table with two foreign keys is a relationship or an entity that
            refers to two others cannot be read from the schema; it is taken
            for a relationship unless named here.

    Returns:
        SchemaIntrospectionResult: vertex and edge tables, the edges stated by
        foreign keys inside vertex tables, and the tables left out with the
        reason for each.

    Raises:
        ValueError: If *entity_tables* names a table the schema does not have,
            or one without a primary key.
    """
    if schema_name is None:
        schema_name = default_schema
    entities = set(entity_tables or ())

    logger.info("Introspecting SQL schema '%s'", schema_name)

    known = {t["table_name"] for t in provider.get_tables(schema_name)}
    for table_name in sorted(entities):
        if table_name not in known:
            raise ValueError(
                f"entity_tables names '{table_name}', which is not a table of "
                f"schema '{schema_name}'"
            )
        if not provider.get_primary_keys(table_name, schema_name):
            raise ValueError(
                f"entity_tables names '{table_name}', which has no primary key "
                "to identify a vertex by"
            )

    vertex_tables = detect_vertex_tables(
        provider, schema_name, default_schema=default_schema, entity_tables=entities
    )
    edge_tables = detect_edge_tables(
        provider, schema_name, default_schema=default_schema, entity_tables=entities
    )
    raw_tables: list[RawTableInfo] = []
    if include_raw_tables:
        raw_tables = build_raw_tables(provider, schema_name)

    skipped_tables = _skipped_tables(
        provider,
        schema_name,
        {t.name for t in vertex_tables} | {t.name for t in edge_tables},
    )
    for skipped in skipped_tables:
        logger.warning(
            "Table '%s' is not part of the inferred graph: %s",
            skipped.name,
            skipped.reason,
        )

    result = SchemaIntrospectionResult(
        vertex_tables=vertex_tables,
        edge_tables=edge_tables,
        raw_tables=raw_tables,
        schema_name=schema_name or "",
        reference_edges=infer_reference_edges(vertex_tables),
        skipped_tables=skipped_tables,
    )

    logger.info(
        f"Found {len(vertex_tables)} vertex-like tables and {len(edge_tables)} edge-like tables"
    )

    return result
