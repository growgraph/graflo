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
from typing import TYPE_CHECKING, Any, Union

from graflo.onto import BaseDataclass

if TYPE_CHECKING:
    from graflo.db.connection.onto import PostgresConfig
else:
    # Import at runtime for type evaluation
    try:
        from graflo.db.connection.onto import PostgresConfig
    except ImportError:
        PostgresConfig = Any  # type: ignore


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
    def get_resource_type(self) -> str:
        """Get the type of resource this pattern matches.

        Returns:
            str: Resource type ("file" or "table")
        """
        pass


@dataclasses.dataclass
class FilePattern(ResourcePattern):
    """Pattern for matching files.

    Attributes:
        regex: Regular expression pattern for matching filenames
        sub_path: Path to search for matching files (default: "./")
    """

    class _(BaseDataclass.Meta):
        tag = "file"

    regex: str | None = None
    sub_path: None | pathlib.Path = dataclasses.field(
        default_factory=lambda: pathlib.Path("./")
    )

    def __post_init__(self):
        """Initialize and validate the file pattern.

        Ensures that sub_path is a Path object and is not None.
        """
        if not isinstance(self.sub_path, pathlib.Path):
            self.sub_path = pathlib.Path(self.sub_path)
        assert self.sub_path is not None

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

    def get_resource_type(self) -> str:
        """Get resource type."""
        return "file"


@dataclasses.dataclass
class TablePattern(ResourcePattern):
    """Pattern for matching database tables.

    Attributes:
        table_name: Exact table name or regex pattern
        schema_name: Schema name (optional, defaults to public)
        database: Database name (optional)
    """

    class _(BaseDataclass.Meta):
        tag = "table"

    table_name: str = ""
    schema_name: str | None = None
    database: str | None = None

    def __post_init__(self):
        """Validate table pattern after initialization."""
        if not self.table_name:
            raise ValueError("table_name is required for TablePattern")

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

    def get_resource_type(self) -> str:
        """Get resource type."""
        return "table"


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

    patterns: dict[str, Union[FilePattern, TablePattern]] = dataclasses.field(
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

    def get_resource_type(self, resource_name: str) -> str | None:
        """Get the resource type for a resource name.

        Args:
            resource_name: Name of the resource

        Returns:
            "file", "table", or None if not found
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
