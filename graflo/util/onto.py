"""Utility ontology classes for resource patterns and configurations.

This module provides data classes for managing resource patterns (files and database tables)
and configurations used throughout the system. These classes support resource discovery,
pattern matching, and configuration management.

Key Components:
    - ResourcePattern: Abstract base class for resource patterns
    - FilePattern: Configuration for file pattern matching
    - TablePattern: Configuration for database table pattern matching
    - Patterns: Collection of named resource patterns with connection management
"""

import abc
import dataclasses
import pathlib
import re
from typing import TYPE_CHECKING, Any

from graflo.onto import BaseDataclass, BaseEnum

if TYPE_CHECKING:
    from graflo.db.connection.onto import PostgresConfig
else:
    # Import at runtime for type evaluation
    try:
        from graflo.db.connection.onto import PostgresConfig
    except ImportError:
        PostgresConfig = Any  # type: ignore


class ResourceType(BaseEnum):
    """Resource types for data sources.

    Resource types distinguish between different data source categories.
    File type detection (CSV, JSON, JSONL, Parquet, etc.) is handled
    automatically by the loader based on file extensions.

    Attributes:
        FILE: File-based data source (any format: CSV, JSON, JSONL, Parquet, etc.)
        SQL_TABLE: SQL database table (e.g., PostgreSQL table)
    """

    FILE = "file"
    SQL_TABLE = "sql_table"


@dataclasses.dataclass
class ResourcePattern(BaseDataclass, abc.ABC):
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


@dataclasses.dataclass
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

    class _(BaseDataclass.Meta):
        tag = "file"

    regex: str | None = None
    sub_path: None | pathlib.Path = dataclasses.field(
        default_factory=lambda: pathlib.Path("./")
    )
    date_field: str | None = None
    date_filter: str | None = None
    date_range_start: str | None = None
    date_range_days: int | None = None

    def __post_init__(self):
        """Initialize and validate the file pattern.

        Ensures that sub_path is a Path object and is not None.
        """
        if not isinstance(self.sub_path, pathlib.Path):
            self.sub_path = pathlib.Path(self.sub_path)
        assert self.sub_path is not None
        # Validate date filtering parameters (note: date filtering for files is not yet implemented)
        if (self.date_filter or self.date_range_start) and not self.date_field:
            raise ValueError(
                "date_field is required when using date_filter or date_range_start"
            )
        if self.date_range_days is not None and not self.date_range_start:
            raise ValueError("date_range_start is required when using date_range_days")

    def matches(self, filename: str) -> bool:
        """Check if pattern matches a filename.

        Args:
            filename: Filename to match

        Returns:
            bool: True if pattern matches
        """
        if self.regex is None:
            return False
        return bool(re.match(self.regex, filename))

    def get_resource_type(self) -> ResourceType:
        """Get resource type.

        FilePattern always represents a FILE resource type.
        The specific file format (CSV, JSON, JSONL, Parquet, etc.) is
        automatically detected by the loader based on file extensions.
        """
        return ResourceType.FILE


@dataclasses.dataclass
class TablePattern(ResourcePattern):
    """Pattern for matching database tables.

    Attributes:
        table_name: Exact table name or regex pattern
        schema_name: Schema name (optional, defaults to public)
        database: Database name (optional)
        date_field: Name of the date field to filter on (for date-based filtering)
        date_filter: SQL-style date filter condition (e.g., "> '2020-10-10'")
        date_range_start: Start date for range filtering (e.g., "2015-11-11")
        date_range_days: Number of days after start date (used with date_range_start)
    """

    class _(BaseDataclass.Meta):
        tag = "table"

    table_name: str = ""
    schema_name: str | None = None
    database: str | None = None
    date_field: str | None = None
    date_filter: str | None = None
    date_range_start: str | None = None
    date_range_days: int | None = None

    def __post_init__(self):
        """Validate table pattern after initialization."""
        if not self.table_name:
            raise ValueError("table_name is required for TablePattern")
        # Validate date filtering parameters
        if (self.date_filter or self.date_range_start) and not self.date_field:
            raise ValueError(
                "date_field is required when using date_filter or date_range_start"
            )
        if self.date_range_days is not None and not self.date_range_start:
            raise ValueError("date_range_start is required when using date_range_days")

    def matches(self, table_identifier: str) -> bool:
        """Check if pattern matches a table name.

        Args:
            table_identifier: Table name to match (format: schema.table or just table)

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

        # Check if table_identifier matches
        if pattern.match(table_identifier):
            return True

        # If schema_name is specified, also check schema.table format
        if self.schema_name:
            full_name = f"{self.schema_name}.{table_identifier}"
            if pattern.match(full_name):
                return True

        return False

    def get_resource_type(self) -> ResourceType:
        """Get resource type."""
        return ResourceType.SQL_TABLE

    def build_where_clause(self) -> str:
        """Build SQL WHERE clause from date filtering parameters.

        Returns:
            WHERE clause string (without the WHERE keyword) or empty string if no filters
        """
        conditions = []

        if self.date_field:
            if self.date_range_start and self.date_range_days is not None:
                # Range filtering: dt >= start_date AND dt < start_date + interval
                # Example: Ingest for k days after 2015-11-11
                conditions.append(
                    f"\"{self.date_field}\" >= '{self.date_range_start}'::date"
                )
                conditions.append(
                    f"\"{self.date_field}\" < '{self.date_range_start}'::date + INTERVAL '{self.date_range_days} days'"
                )
            elif self.date_filter:
                # Direct filter: dt > 2020-10-10 or dt > '2020-10-10'
                # The date_filter should include the operator and value
                # If value doesn't have quotes, add them
                filter_parts = self.date_filter.strip().split(None, 1)
                if len(filter_parts) == 2:
                    operator, value = filter_parts
                    # Add quotes if not already present and value looks like a date
                    if not (value.startswith("'") and value.endswith("'")):
                        # Check if it's a date-like string (YYYY-MM-DD format)
                        if len(value) == 10 and value.count("-") == 2:
                            value = f"'{value}'"
                    conditions.append(f'"{self.date_field}" {operator} {value}')
                else:
                    # If format is unexpected, use as-is
                    conditions.append(f'"{self.date_field}" {self.date_filter}')

        if conditions:
            return " AND ".join(conditions)
        return ""


@dataclasses.dataclass
class Patterns(BaseDataclass):
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
        patterns: Dictionary mapping resource names to ResourcePattern instances
        postgres_configs: Dictionary mapping (config_key, schema_name) to PostgresConfig
        postgres_table_configs: Dictionary mapping resource_name to (config_key, schema_name, table_name)
    """

    patterns: dict[str, TablePattern | FilePattern] = dataclasses.field(
        default_factory=dict
    )
    postgres_configs: dict[tuple[str, str | None], Any] = dataclasses.field(
        default_factory=dict, metadata={"exclude": True}
    )
    postgres_table_configs: dict[str, tuple[str, str | None, str]] = dataclasses.field(
        default_factory=dict, metadata={"exclude": True}
    )
    # Initialization parameters (not stored as fields, excluded from serialization)
    # Use Any for _postgres_connections to avoid type evaluation issues with dataclass_wizard
    _resource_mapping: dict[str, str | tuple[str, str]] | None = dataclasses.field(
        default=None, repr=False, compare=False, metadata={"exclude": True}
    )
    _postgres_connections: dict[str, Any] | None = dataclasses.field(
        default=None, repr=False, compare=False, metadata={"exclude": True}
    )
    _postgres_tables: dict[str, tuple[str, str | None, str]] | None = dataclasses.field(
        default=None, repr=False, compare=False, metadata={"exclude": True}
    )

    def __post_init__(self):
        """Initialize Patterns from resource mappings and PostgreSQL configurations."""
        # Store PostgreSQL connection configs
        if self._postgres_connections:
            for config_key, config in self._postgres_connections.items():
                if config is not None:
                    schema_name = config.schema_name
                    self.postgres_configs[(config_key, schema_name)] = config

        # Process resource mappings
        if self._resource_mapping:
            for resource_name, resource_spec in self._resource_mapping.items():
                if isinstance(resource_spec, str):
                    # File path - create FilePattern
                    file_path = pathlib.Path(resource_spec)
                    pattern = FilePattern(
                        regex=f"^{re.escape(file_path.name)}$",
                        sub_path=file_path.parent,
                        resource_name=resource_name,
                    )
                    self.patterns[resource_name] = pattern
                elif isinstance(resource_spec, tuple) and len(resource_spec) == 2:
                    # (config_key, table_name) tuple - create TablePattern
                    config_key, table_name = resource_spec
                    # Find the schema_name from the config
                    config = (
                        self._postgres_connections.get(config_key)
                        if self._postgres_connections
                        else None
                    )
                    schema_name = config.schema_name if config else None

                    pattern = TablePattern(
                        table_name=table_name,
                        schema_name=schema_name,
                        resource_name=resource_name,
                    )
                    self.patterns[resource_name] = pattern
                    # Store the config mapping
                    self.postgres_table_configs[resource_name] = (
                        config_key,
                        schema_name,
                        table_name,
                    )

        # Process explicit postgres_tables mapping
        if self._postgres_tables:
            for table_name, (
                config_key,
                schema_name,
                actual_table_name,
            ) in self._postgres_tables.items():
                pattern = TablePattern(
                    table_name=actual_table_name,
                    schema_name=schema_name,
                    resource_name=table_name,
                )
                self.patterns[table_name] = pattern
                self.postgres_table_configs[table_name] = (
                    config_key,
                    schema_name,
                    actual_table_name,
                )

    def add_file_pattern(self, name: str, file_pattern: FilePattern):
        """Add a file pattern to the collection.

        Args:
            name: Name of the pattern
            file_pattern: FilePattern instance
        """
        self.patterns[name] = file_pattern

    def add_table_pattern(self, name: str, table_pattern: TablePattern):
        """Add a table pattern to the collection.

        Args:
            name: Name of the pattern
            table_pattern: TablePattern instance
        """
        self.patterns[name] = table_pattern

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
        if resource_name in self.patterns:
            return self.patterns[resource_name].get_resource_type()
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
