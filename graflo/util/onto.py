"""Utility ontology classes for resource patterns and configurations.

This module provides Pydantic models for managing resource patterns (files and database tables)
and configurations used throughout the system. These classes support resource discovery,
pattern matching, and configuration management.

Key Components:
    - ResourcePattern: Abstract base class for resource patterns
    - FilePattern: Configuration for file pattern matching
    - TablePattern: Configuration for database table pattern matching
    - Patterns: Collection of named resource patterns with connection management
"""

import abc
import copy
import pathlib
import re
from typing import TYPE_CHECKING, Any, Self

from pydantic import AliasChoices, Field, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import BaseEnum

if TYPE_CHECKING:
    from graflo.db import PostgresConfig
else:
    # Import at runtime for type evaluation
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


class ResourcePattern(ConfigBaseModel, abc.ABC):
    """Abstract base class for resource patterns (files or tables).

    Provides common API for pattern matching and resource identification.
    All concrete pattern types inherit from this class.

    Attributes:
        resource_name: Name of the resource this pattern matches
    """

    resource_name: str | None = None

    @abc.abstractmethod
    def matches(self, resource_identifier: str) -> bool:
        """Check if pattern matches a resource identifier.

        Args:
            resource_identifier: Identifier to match (filename or table name)

        Returns:
            bool: True if pattern matches
        """
        pass

    @abc.abstractmethod
    def get_resource_type(self) -> ResourceType:
        """Get the type of resource this pattern matches.

        Returns:
            ResourceType: Resource type enum value
        """
        pass


class FilePattern(ResourcePattern):
    """Pattern for matching files.

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
    def _validate_file_pattern(self) -> Self:
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
        """Check if pattern matches a filename.

        Args:
            resource_identifier: Filename to match

        Returns:
            bool: True if pattern matches
        """
        if self.regex is None:
            return False
        return bool(re.match(self.regex, resource_identifier))

    def get_resource_type(self) -> ResourceType:
        """Get resource type.

        FilePattern always represents a FILE resource type.
        The specific file format (CSV, JSON, JSONL, Parquet, etc.) is
        automatically detected by the loader based on file extensions.
        """
        return ResourceType.FILE


class JoinClause(ConfigBaseModel):
    """Specification for a SQL JOIN operation.

    Used by TablePattern to describe multi-table queries. Each JoinClause
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


class TablePattern(ResourcePattern):
    """Pattern for matching database tables.

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

    @model_validator(mode="after")
    def _validate_table_pattern(self) -> Self:
        """Validate table_name and date filtering parameters."""
        if not self.table_name:
            raise ValueError("table_name is required for TablePattern")
        if (self.date_filter or self.date_range_start) and not self.date_field:
            raise ValueError(
                "date_field is required when using date_filter or date_range_start"
            )
        if self.date_range_days is not None and not self.date_range_start:
            raise ValueError("date_range_start is required when using date_range_days")
        return self

    def matches(self, resource_identifier: str) -> bool:
        """Check if pattern matches a table name.

        Args:
            resource_identifier: Table name to match (format: schema.table or just table)

        Returns:
            bool: True if pattern matches
        """
        if not self.table_name:
            return False

        # Compile regex pattern
        if self.table_name.startswith("^") or self.table_name.endswith("$"):
            # Already a regex pattern
            pattern = re.compile(self.table_name)
        else:
            # Exact match pattern
            pattern = re.compile(f"^{re.escape(self.table_name)}$")

        # Check if resource_identifier matches
        if pattern.match(resource_identifier):
            return True

        # If schema_name is specified, also check schema.table format
        if self.schema_name:
            full_name = f"{self.schema_name}.{resource_identifier}"
            if pattern.match(full_name):
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

    # ------------------------------------------------------------------
    # Full SQL query builder (handles SELECT, FROM, JOINs, WHERE)
    # ------------------------------------------------------------------

    def build_query(self, effective_schema: str | None = None) -> str:
        """Build a complete SQL SELECT query.

        Incorporates the base table, any JoinClauses, explicit select_columns,
        date filters, and FilterExpression filters.

        Args:
            effective_schema: Schema to use if ``self.schema_name`` is None.

        Returns:
            Complete SQL query string.
        """
        schema = self.schema_name or effective_schema or "public"
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


class SparqlPattern(ResourcePattern):
    """Pattern for matching SPARQL / RDF data sources.

    Each ``SparqlPattern`` targets instances of a single ``rdf:Class``.
    It can be backed either by a remote SPARQL endpoint (Fuseki, Blazegraph, ...)
    or by a local RDF file parsed with *rdflib*.

    Attributes:
        rdf_class: Full URI of the ``rdf:Class`` whose instances this pattern
            fetches (e.g. ``"http://example.org/Person"``).
        endpoint_url: SPARQL query endpoint URL.  When set, instances are
            fetched via HTTP.  When ``None`` the pattern is for local file mode.
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


class Patterns(ConfigBaseModel):
    """Collection of named resource patterns with connection management.

    This class manages a collection of resource patterns (files or tables),
    each associated with a name. It efficiently handles PostgreSQL connections
    by grouping tables that share the same connection configuration.

    The constructor accepts:
    - resource_mapping: dict mapping resource_name -> (file_path or table_name)
    - postgres_connections: dict mapping config_key -> PostgresConfig
      where config_key identifies a connection configuration
    - postgres_tables: dict mapping table_name -> (config_key, schema_name, table_name)

    Attributes:
        file_patterns: Dictionary mapping resource names to FilePattern instances
        table_patterns: Dictionary mapping resource names to TablePattern instances
        sparql_patterns: Dictionary mapping resource names to SparqlPattern instances
        patterns: Property that merges all pattern dicts (for backward compatibility)
        postgres_configs: Dictionary mapping (config_key, schema_name) to PostgresConfig
        postgres_table_configs: Dictionary mapping resource_name to (config_key, schema_name, table_name)
        sparql_configs: Dictionary mapping config_key to SparqlEndpointConfig
    """

    file_patterns: dict[str, FilePattern] = Field(default_factory=dict)
    table_patterns: dict[str, TablePattern] = Field(default_factory=dict)
    sparql_patterns: dict[str, SparqlPattern] = Field(default_factory=dict)
    postgres_configs: dict[tuple[str, str | None], Any] = Field(
        default_factory=dict, exclude=True
    )
    postgres_table_configs: dict[str, tuple[str, str | None, str]] = Field(
        default_factory=dict, exclude=True
    )
    sparql_configs: dict[str, Any] = Field(default_factory=dict, exclude=True)
    # Initialization parameters (not stored in serialization); accept both _name and name
    resource_mapping: dict[str, str | tuple[str, str]] | None = Field(
        default=None,
        exclude=True,
        validation_alias=AliasChoices("_resource_mapping", "resource_mapping"),
    )
    postgres_connections: dict[str, Any] | None = Field(
        default=None,
        exclude=True,
        validation_alias=AliasChoices("_postgres_connections", "postgres_connections"),
    )
    postgres_tables: dict[str, tuple[str, str | None, str]] | None = Field(
        default=None,
        exclude=True,
        validation_alias=AliasChoices("_postgres_tables", "postgres_tables"),
    )

    @property
    def patterns(self) -> dict[str, TablePattern | FilePattern | SparqlPattern]:
        """Merged dictionary of all patterns (file, table, and SPARQL).

        Returns:
            Dictionary mapping resource names to ResourcePattern instances
        """
        result: dict[str, TablePattern | FilePattern | SparqlPattern] = {}
        result.update(self.file_patterns)
        result.update(self.table_patterns)
        result.update(self.sparql_patterns)
        return result

    @model_validator(mode="after")
    def _populate_from_mappings(self) -> Self:
        """Populate file_patterns/table_patterns from resource mappings and PostgreSQL configs."""
        if self.postgres_connections:
            for config_key, config in self.postgres_connections.items():
                if config is not None:
                    schema_name = getattr(config, "schema_name", None)
                    self.postgres_configs[(config_key, schema_name)] = config

        if self.resource_mapping:
            for resource_name, resource_spec in self.resource_mapping.items():
                if isinstance(resource_spec, str):
                    file_path = pathlib.Path(resource_spec)
                    pattern = FilePattern(
                        regex=f"^{re.escape(file_path.name)}$",
                        sub_path=file_path.parent,
                        resource_name=resource_name,
                    )
                    self.file_patterns[resource_name] = pattern
                elif isinstance(resource_spec, tuple) and len(resource_spec) == 2:
                    config_key, table_name = resource_spec
                    config = (
                        self.postgres_connections.get(config_key)
                        if self.postgres_connections
                        else None
                    )
                    schema_name = (
                        getattr(config, "schema_name", None) if config else None
                    )
                    pattern = TablePattern(
                        table_name=table_name,
                        schema_name=schema_name,
                        resource_name=resource_name,
                    )
                    self.table_patterns[resource_name] = pattern
                    self.postgres_table_configs[resource_name] = (
                        config_key,
                        schema_name,
                        table_name,
                    )

        if self.postgres_tables:
            for table_name, (
                config_key,
                schema_name,
                actual_table_name,
            ) in self.postgres_tables.items():
                pattern = TablePattern(
                    table_name=actual_table_name,
                    schema_name=schema_name,
                    resource_name=table_name,
                )
                self.table_patterns[table_name] = pattern
                self.postgres_table_configs[table_name] = (
                    config_key,
                    schema_name,
                    actual_table_name,
                )
        return self

    @classmethod
    def from_dict(cls, data: dict[str, Any] | list[Any]) -> Self:
        """Create Patterns from dictionary, supporting both old and new YAML formats.

        Supports two formats:
        1. New format: Separate `file_patterns` and `table_patterns` fields
        2. Old format: Unified `patterns` field with `__tag__` markers (for backward compatibility)

        Args:
            data: Dictionary containing patterns data (or list for base compatibility)

        Returns:
            Patterns: New Patterns instance with properly deserialized patterns
        """
        if isinstance(data, list):
            return cls.model_validate(data)
        if (
            "file_patterns" in data
            or "table_patterns" in data
            or "sparql_patterns" in data
        ):
            # Strip __tag__ from nested pattern dicts so extra="forbid" does not fail
            data = copy.deepcopy(data)
            for key in ("file_patterns", "table_patterns", "sparql_patterns"):
                if key in data and isinstance(data[key], dict):
                    for name, val in data[key].items():
                        if isinstance(val, dict) and "__tag__" in val:
                            data[key][name] = {
                                k: v for k, v in val.items() if k != "__tag__"
                            }
            return cls.model_validate(data)

        patterns_data = data.get("patterns", {})
        data_copy = {k: v for k, v in data.items() if k != "patterns"}
        instance = cls.model_validate(data_copy)

        for pattern_name, raw in patterns_data.items():
            if raw is None:
                continue
            pattern_dict = {k: v for k, v in raw.items() if k != "__tag__"}
            tag_val = raw.get("__tag__") if isinstance(raw, dict) else None
            if tag_val == "file":
                instance.file_patterns[pattern_name] = FilePattern.model_validate(
                    pattern_dict
                )
            elif tag_val == "table":
                instance.table_patterns[pattern_name] = TablePattern.model_validate(
                    pattern_dict
                )
            elif tag_val == "sparql":
                instance.sparql_patterns[pattern_name] = SparqlPattern.model_validate(
                    pattern_dict
                )
            else:
                if "table_name" in pattern_dict:
                    instance.table_patterns[pattern_name] = TablePattern.model_validate(
                        pattern_dict
                    )
                elif "regex" in pattern_dict or "sub_path" in pattern_dict:
                    instance.file_patterns[pattern_name] = FilePattern.model_validate(
                        pattern_dict
                    )
                elif "rdf_class" in pattern_dict:
                    instance.sparql_patterns[pattern_name] = (
                        SparqlPattern.model_validate(pattern_dict)
                    )
                else:
                    raise ValueError(
                        f"Unable to determine pattern type for '{pattern_name}'. "
                        "Expected '__tag__: file|table|sparql', "
                        "or pattern fields (table_name for TablePattern, "
                        "regex/sub_path for FilePattern, "
                        "rdf_class for SparqlPattern)"
                    )
        return instance

    def add_file_pattern(self, name: str, file_pattern: FilePattern):
        """Add a file pattern to the collection.

        Args:
            name: Name of the pattern
            file_pattern: FilePattern instance
        """
        self.file_patterns[name] = file_pattern

    def add_table_pattern(self, name: str, table_pattern: TablePattern):
        """Add a table pattern to the collection.

        Args:
            name: Name of the pattern
            table_pattern: TablePattern instance
        """
        self.table_patterns[name] = table_pattern

    def add_sparql_pattern(self, name: str, sparql_pattern: SparqlPattern):
        """Add a SPARQL pattern to the collection.

        Args:
            name: Name of the pattern (typically the rdf:Class local name)
            sparql_pattern: SparqlPattern instance
        """
        self.sparql_patterns[name] = sparql_pattern

    def get_sparql_config(self, resource_name: str) -> Any:
        """Get SPARQL endpoint config for a resource.

        Args:
            resource_name: Name of the resource

        Returns:
            SparqlEndpointConfig if resource is a SPARQL pattern, None otherwise
        """
        if resource_name in self.sparql_patterns:
            pattern = self.sparql_patterns[resource_name]
            if pattern.endpoint_url:
                for cfg in self.sparql_configs.values():
                    if (
                        hasattr(cfg, "query_endpoint")
                        and cfg.query_endpoint == pattern.endpoint_url
                    ):
                        return cfg
            # Return the first config if only one is registered
            if self.sparql_configs:
                return next(iter(self.sparql_configs.values()))
        return None

    def get_postgres_config(self, resource_name: str) -> Any:
        """Get PostgreSQL connection config for a resource.

        Args:
            resource_name: Name of the resource

        Returns:
            PostgresConfig if resource is a PostgreSQL table, None otherwise
        """
        if resource_name in self.postgres_table_configs:
            config_key, schema_name, _ = self.postgres_table_configs[resource_name]
            return self.postgres_configs.get((config_key, schema_name))
        return None

    def get_resource_type(self, resource_name: str) -> ResourceType | None:
        """Get the resource type for a resource name.

        Args:
            resource_name: Name of the resource

        Returns:
            ResourceType enum value or None if not found
        """
        if resource_name in self.file_patterns:
            return self.file_patterns[resource_name].get_resource_type()
        if resource_name in self.table_patterns:
            return self.table_patterns[resource_name].get_resource_type()
        if resource_name in self.sparql_patterns:
            return self.sparql_patterns[resource_name].get_resource_type()
        return None

    def get_table_info(self, resource_name: str) -> tuple[str, str | None] | None:
        """Get table name and schema for a PostgreSQL table resource.

        Args:
            resource_name: Name of the resource

        Returns:
            Tuple of (table_name, schema_name) or None if not a table resource
        """
        if resource_name in self.postgres_table_configs:
            _, schema_name, table_name = self.postgres_table_configs[resource_name]
            return (table_name, schema_name)
        return None
