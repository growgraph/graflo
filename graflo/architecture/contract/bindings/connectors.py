"""Resource connector types (file, SQL table, SPARQL)."""

from __future__ import annotations

import abc
import pathlib
import re
from typing import TYPE_CHECKING, Any, Self

from pydantic import Field, field_validator, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import BaseEnum

if TYPE_CHECKING:
    from graflo.db import PostgresConfig
else:
    try:
        from graflo.db import PostgresConfig
    except ImportError:
        PostgresConfig = Any


class ResourceType(BaseEnum):
    """Resource types for data sources.

    Resource types distinguish between different data source categories.
    File type detection (CSV, JSON, JSONL, Parquet, etc.) is handled
    automatically by the loader based on file extensions.

    Attributes:
        FILE: File-based data source (any format: CSV, JSON, JSONL, Parquet, etc.)
        SQL_TABLE: SQL database table (e.g., PostgreSQL table)
        SPARQL: SPARQL / RDF data source (endpoint or .ttl/.rdf files via rdflib)
    """

    FILE = "file"
    SQL_TABLE = "sql_table"
    SPARQL = "sparql"


class ResourceConnector(ConfigBaseModel, abc.ABC):
    """Abstract base class for resource connectors (files or tables).

    Provides common API for connector matching and resource identification.
    All concrete connector types inherit from this class.

    Attributes:
        resource_name: Name of the resource this connector matches
    """

    resource_name: str | None = None

    @abc.abstractmethod
    def matches(self, resource_identifier: str) -> bool:
        """Check if connector matches a resource identifier.

        Args:
            resource_identifier: Identifier to match (filename or table name)

        Returns:
            bool: True if connector matches
        """
        pass

    @abc.abstractmethod
    def get_resource_type(self) -> ResourceType:
        """Get the type of resource this connector matches.

        Returns:
            ResourceType: Resource type enum value
        """
        pass


class FileConnector(ResourceConnector):
    """Connector for matching files.

    Attributes:
        regex: Regular expression pattern for matching filenames
        sub_path: Path to search for matching files (default: "./")
        date_field: Name of the date field to filter on (for date-based filtering)
        date_filter: SQL-style date filter condition (e.g., "> '2020-10-10'")
        date_range_start: Start date for range filtering (e.g., "2015-11-11")
        date_range_days: Number of days after start date (used with date_range_start)
    """

    regex: str | None = None
    sub_path: pathlib.Path = Field(default_factory=lambda: pathlib.Path("./"))
    date_field: str | None = None
    date_filter: str | None = None
    date_range_start: str | None = None
    date_range_days: int | None = None

    @model_validator(mode="after")
    def _validate_file_connector(self) -> Self:
        """Ensure sub_path is a Path and validate date filtering parameters."""
        if not isinstance(self.sub_path, pathlib.Path):
            object.__setattr__(self, "sub_path", pathlib.Path(self.sub_path))
        if (self.date_filter or self.date_range_start) and not self.date_field:
            raise ValueError(
                "date_field is required when using date_filter or date_range_start"
            )
        if self.date_range_days is not None and not self.date_range_start:
            raise ValueError("date_range_start is required when using date_range_days")
        return self

    def matches(self, resource_identifier: str) -> bool:
        """Check if connector matches a filename.

        Args:
            resource_identifier: Filename to match

        Returns:
            bool: True if connector matches
        """
        if self.regex is None:
            return False
        return bool(re.match(self.regex, resource_identifier))

    def get_resource_type(self) -> ResourceType:
        """Get resource type.

        FileConnector always represents a FILE resource type.
        The specific file format (CSV, JSON, JSONL, Parquet, etc.) is
        automatically detected by the loader based on file extensions.
        """
        return ResourceType.FILE


class JoinClause(ConfigBaseModel):
    """Specification for a SQL JOIN operation.

    Used by TableConnector to describe multi-table queries. Each JoinClause
    adds one JOIN to the generated SQL.

    Attributes:
        table: Table name to join (e.g. "all_classes").
        schema_name: Optional schema override for the joined table.
        alias: SQL alias for the joined table (e.g. "s", "t"). Required when
            the same table is joined more than once.
        on_self: Column on the base (left) table used in the ON condition.
        on_other: Column on the joined (right) table used in the ON condition.
        join_type: Type of join -- LEFT, INNER, etc. Defaults to LEFT.
        select_fields: Explicit list of columns to SELECT from this join.
            When None every column of the joined table is included (aliased
            with the join alias prefix).
    """

    table: str = Field(..., description="Table name to join.")
    schema_name: str | None = Field(
        default=None, description="Schema override for the joined table."
    )
    alias: str | None = Field(
        default=None, description="SQL alias for the joined table."
    )
    on_self: str = Field(
        ..., description="Column on the base table for the ON condition."
    )
    on_other: str = Field(
        ..., description="Column on the joined table for the ON condition."
    )
    join_type: str = Field(default="LEFT", description="JOIN type (LEFT, INNER, etc.).")
    select_fields: list[str] | None = Field(
        default=None,
        description="Columns to SELECT from this join (None = all columns).",
    )


class TableConnector(ResourceConnector):
    """Connector for matching database tables.

    Supports simple single-table queries as well as multi-table JOINs and
    pushdown filters via ``FilterExpression``.

    Attributes:
        table_name: Exact table name or regex pattern
        schema_name: Schema name (optional, defaults to public)
        database: Database name (optional)
        date_field: Name of the date field to filter on (for date-based filtering)
        date_filter: SQL-style date filter condition (e.g., "> '2020-10-10'")
        date_range_start: Start date for range filtering (e.g., "2015-11-11")
        date_range_days: Number of days after start date (used with date_range_start)
        filters: General-purpose pushdown filters rendered as SQL WHERE fragments.
        joins: Multi-table JOIN specifications (auto-generated or explicit).
        select_columns: Explicit SELECT column list. None means ``*`` for the
            base table (plus aliased columns from joins).
    """

    table_name: str = ""
    schema_name: str | None = None
    database: str | None = None
    date_field: str | None = None
    date_filter: str | None = None
    date_range_start: str | None = None
    date_range_days: int | None = None
    filters: list[Any] = Field(
        default_factory=list,
        description="Pushdown FilterExpression filters (rendered to SQL WHERE).",
    )
    joins: list[JoinClause] = Field(
        default_factory=list,
        description="JOIN clauses for multi-table queries.",
    )
    select_columns: list[str] | None = Field(
        default=None,
        description="Explicit SELECT columns. None = SELECT * (plus join aliases).",
    )
    view: Any = Field(
        default=None,
        description="SelectSpec or dict for declarative view (alternative to table+joins+filters).",
    )

    @field_validator("view", mode="before")
    @classmethod
    def _coerce_view(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, dict):
            from graflo.filter.select import SelectSpec

            return SelectSpec.from_dict(v)
        return v

    @model_validator(mode="after")
    def _validate_table_connector(self) -> Self:
        """Validate table_name and date filtering parameters."""
        if not self.table_name:
            raise ValueError("table_name is required for TableConnector")
        if (self.date_filter or self.date_range_start) and not self.date_field:
            raise ValueError(
                "date_field is required when using date_filter or date_range_start"
            )
        if self.date_range_days is not None and not self.date_range_start:
            raise ValueError("date_range_start is required when using date_range_days")
        return self

    def matches(self, resource_identifier: str) -> bool:
        """Check if connector matches a table name.

        Args:
            resource_identifier: Table name to match (format: schema.table or just table)

        Returns:
            bool: True if connector matches
        """
        if not self.table_name:
            return False

        # Compile regex expression
        if self.table_name.startswith("^") or self.table_name.endswith("$"):
            # Already a regex expression
            compiled_regex = re.compile(self.table_name)
        else:
            # Exact match expression
            compiled_regex = re.compile(f"^{re.escape(self.table_name)}$")

        # Check if resource_identifier matches
        if compiled_regex.match(resource_identifier):
            return True

        # If schema_name is specified, also check schema.table format
        if self.schema_name:
            full_name = f"{self.schema_name}.{resource_identifier}"
            if compiled_regex.match(full_name):
                return True

        return False

    def get_resource_type(self) -> ResourceType:
        """Get resource type."""
        return ResourceType.SQL_TABLE

    def build_where_clause(self) -> str:
        """Build SQL WHERE clause from date filtering parameters **and** general filters.

        Returns:
            WHERE clause string (without the WHERE keyword) or empty string if no filters
        """
        from graflo.filter.onto import FilterExpression
        from graflo.onto import ExpressionFlavor

        conditions: list[str] = []

        # Date-specific conditions (legacy fields)
        if self.date_field:
            if self.date_range_start and self.date_range_days is not None:
                conditions.append(
                    f"\"{self.date_field}\" >= '{self.date_range_start}'::date"
                )
                conditions.append(
                    f"\"{self.date_field}\" < '{self.date_range_start}'::date + INTERVAL '{self.date_range_days} days'"
                )
            elif self.date_filter:
                filter_parts = self.date_filter.strip().split(None, 1)
                if len(filter_parts) == 2:
                    operator, value = filter_parts
                    if not (value.startswith("'") and value.endswith("'")):
                        if len(value) == 10 and value.count("-") == 2:
                            value = f"'{value}'"
                    conditions.append(f'"{self.date_field}" {operator} {value}')
                else:
                    conditions.append(f'"{self.date_field}" {self.date_filter}')

        # General-purpose FilterExpression filters
        for filt in self.filters:
            if isinstance(filt, FilterExpression):
                rendered = filt(kind=ExpressionFlavor.SQL)
                if rendered:
                    conditions.append(str(rendered))

        if conditions:
            return " AND ".join(conditions)
        return ""

    def build_query(self, effective_schema: str | None = None) -> str:
        """Build a complete SQL SELECT query.

        When ``view`` is set, delegates to ``view.build_sql()``. Otherwise
        incorporates the base table, any JoinClauses, explicit select_columns,
        date filters, and FilterExpression filters.

        Args:
            effective_schema: Schema to use if ``self.schema_name`` is None.

        Returns:
            Complete SQL query string.
        """
        schema = self.schema_name or effective_schema or "public"
        if self.view is not None:
            from graflo.filter.select import SelectSpec

            if isinstance(self.view, SelectSpec):
                return self.view.build_sql(schema=schema, base_table=self.table_name)
        base_alias = "r" if self.joins else None
        base_ref = f'"{schema}"."{self.table_name}"'
        if base_alias:
            base_ref_aliased = f"{base_ref} {base_alias}"
        else:
            base_ref_aliased = base_ref

        # --- SELECT ---
        select_parts: list[str] = []
        if self.select_columns is not None:
            select_parts = list(self.select_columns)
        elif self.joins:
            select_parts.append(f"{base_alias}.*")
            for jc in self.joins:
                alias = jc.alias or jc.table
                jc_schema = jc.schema_name or schema
                if jc.select_fields is not None:
                    for col in jc.select_fields:
                        select_parts.append(f'{alias}."{col}" AS "{alias}__{col}"')
                else:
                    select_parts.append(f"{alias}.*")
        else:
            select_parts.append("*")

        select_clause = ", ".join(select_parts)

        # --- FROM + JOINs ---
        from_clause = base_ref_aliased
        for jc in self.joins:
            jc_schema = jc.schema_name or schema
            alias = jc.alias or jc.table
            join_ref = f'"{jc_schema}"."{jc.table}"'
            left_col = (
                f'{base_alias}."{jc.on_self}"' if base_alias else f'"{jc.on_self}"'
            )
            right_col = f'{alias}."{jc.on_other}"'
            from_clause += (
                f" {jc.join_type} JOIN {join_ref} {alias} ON {left_col} = {right_col}"
            )

        query = f"SELECT {select_clause} FROM {from_clause}"

        # --- WHERE ---
        where = self.build_where_clause()
        if where:
            query += f" WHERE {where}"

        return query


class SparqlConnector(ResourceConnector):
    """Connector for matching SPARQL / RDF data sources.

    Each ``SparqlConnector`` targets instances of a single ``rdf:Class``.
    It can be backed either by a remote SPARQL endpoint (Fuseki, Blazegraph, ...)
    or by a local RDF file parsed with *rdflib*.

    Attributes:
        rdf_class: Full URI of the ``rdf:Class`` whose instances this connector
            fetches (e.g. ``"http://example.org/Person"``).
        endpoint_url: SPARQL query endpoint URL.  When set, instances are
            fetched via HTTP.  When ``None`` the connector is for local file mode.
        graph_uri: Named-graph URI to restrict the query to (optional).
        sparql_query: Custom SPARQL ``SELECT`` query override.  When provided
            the auto-generated per-class query is skipped.
        rdf_file: Path to a local RDF file (``.ttl``, ``.rdf``, ``.n3``,
            ``.jsonld``).  Mutually exclusive with *endpoint_url*.
    """

    rdf_class: str = Field(
        ..., description="URI of the rdf:Class to fetch instances of"
    )
    endpoint_url: str | None = Field(
        default=None, description="SPARQL query endpoint URL"
    )
    graph_uri: str | None = Field(
        default=None, description="Named graph URI (optional)"
    )
    sparql_query: str | None = Field(
        default=None, description="Custom SPARQL query override"
    )
    rdf_file: pathlib.Path | None = Field(
        default=None, description="Path to a local RDF file"
    )

    def matches(self, resource_identifier: str) -> bool:
        """Match by the local name (fragment) of the rdf:Class URI.

        Args:
            resource_identifier: Identifier to match against

        Returns:
            True when *resource_identifier* equals the class local name
        """
        local_name = self.rdf_class.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
        return resource_identifier == local_name

    def get_resource_type(self) -> ResourceType:
        """Return ``ResourceType.SPARQL``."""
        return ResourceType.SPARQL

    def build_select_query(self) -> str:
        """Build a SPARQL SELECT query for instances of ``rdf_class``.

        If *sparql_query* is set it is returned as-is.  Otherwise a simple
        per-class query is generated::

            SELECT ?s ?p ?o WHERE {
              ?s a <rdf_class> .
              ?s ?p ?o .
            }

        Returns:
            SPARQL query string
        """
        if self.sparql_query:
            return self.sparql_query

        graph_open = f"GRAPH <{self.graph_uri}> {{" if self.graph_uri else ""
        graph_close = "}" if self.graph_uri else ""

        return (
            "SELECT ?s ?p ?o WHERE { "
            f"{graph_open} "
            f"?s a <{self.rdf_class}> . "
            f"?s ?p ?o . "
            f"{graph_close} "
            "}"
        )
