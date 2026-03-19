"""Named connector collections and connection wiring."""

from __future__ import annotations

import copy
import pathlib
import re
from typing import Any, Self

from pydantic import AliasChoices, Field, model_validator

from graflo.architecture.base import ConfigBaseModel
from .connectors import (
    FileConnector,
    ResourceType,
    SparqlConnector,
    TableConnector,
)


class Bindings(ConfigBaseModel):
    """Collection of named resource connectors with connection management.

    This class manages a collection of resource connectors (files or tables),
    each associated with a name. It efficiently handles PostgreSQL connections
    by grouping tables that share the same connection configuration.

    The constructor accepts:
    - resource_mapping: dict mapping resource_name -> (file_path or table_name)
    - postgres_connections: dict mapping config_key -> PostgresConfig
      where config_key identifies a connection configuration
    - postgres_tables: dict mapping table_name -> (config_key, schema_name, table_name)

    Attributes:
        file_connectors: Dictionary mapping resource names to FileConnector instances
        table_connectors: Dictionary mapping resource names to TableConnector instances
        sparql_connectors: Dictionary mapping resource names to SparqlConnector instances
        connectors: Property that merges all connector dicts (for backward compatibility)
        postgres_configs: Dictionary mapping (config_key, schema_name) to PostgresConfig
        postgres_table_configs: Dictionary mapping resource_name to (config_key, schema_name, table_name)
        sparql_configs: Dictionary mapping config_key to SparqlEndpointConfig
    """

    file_connectors: dict[str, FileConnector] = Field(default_factory=dict)
    table_connectors: dict[str, TableConnector] = Field(default_factory=dict)
    sparql_connectors: dict[str, SparqlConnector] = Field(default_factory=dict)
    postgres_configs: dict[tuple[str, str | None], Any] = Field(
        default_factory=dict, exclude=True
    )
    postgres_table_configs: dict[str, tuple[str, str | None, str]] = Field(
        default_factory=dict, exclude=True
    )
    sparql_configs: dict[str, Any] = Field(default_factory=dict, exclude=True)
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
    def connectors(self) -> dict[str, TableConnector | FileConnector | SparqlConnector]:
        """Merged dictionary of all connectors (file, table, and SPARQL).

        Returns:
            Dictionary mapping resource names to ResourceConnector instances
        """
        result: dict[str, TableConnector | FileConnector | SparqlConnector] = {}
        result.update(self.file_connectors)
        result.update(self.table_connectors)
        result.update(self.sparql_connectors)
        return result

    @model_validator(mode="after")
    def _populate_from_mappings(self) -> Self:
        """Populate file_connectors/table_connectors from resource mappings and PostgreSQL configs."""
        if self.postgres_connections:
            raise ValueError(
                "Inline postgres_connections are not supported in Bindings. "
                "Resolve credentials externally at runtime."
            )
        if self.sparql_configs:
            raise ValueError(
                "Inline sparql_configs are not supported in Bindings. "
                "Resolve credentials externally at runtime."
            )
        if self.resource_mapping:
            for resource_name, resource_spec in self.resource_mapping.items():
                if isinstance(resource_spec, str):
                    file_path = pathlib.Path(resource_spec)
                    connector = FileConnector(
                        regex=f"^{re.escape(file_path.name)}$",
                        sub_path=file_path.parent,
                        resource_name=resource_name,
                    )
                    self.file_connectors[resource_name] = connector
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
                    connector = TableConnector(
                        table_name=table_name,
                        schema_name=schema_name,
                        resource_name=resource_name,
                    )
                    self.table_connectors[resource_name] = connector
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
                connector = TableConnector(
                    table_name=actual_table_name,
                    schema_name=schema_name,
                    resource_name=table_name,
                )
                self.table_connectors[table_name] = connector
                self.postgres_table_configs[table_name] = (
                    config_key,
                    schema_name,
                    actual_table_name,
                )
        return self

    @classmethod
    def from_dict(cls, data: dict[str, Any] | list[Any]) -> Self:
        """Create Bindings from dictionary, supporting both old and new YAML formats.

        Supports two formats:
        1. New format: Separate `file_connectors` and `table_connectors` fields
        2. Legacy format: Unified `patterns` field with `__tag__` markers (for backward compatibility)

        Args:
            data: Dictionary containing connector data (or list for base compatibility)

        Returns:
            Bindings: New Bindings instance with properly deserialized connectors
        """
        if isinstance(data, list):
            return cls.model_validate(data)
        forbidden_credential_keys = {
            "postgres_connections",
            "_postgres_connections",
            "sparql_configs",
        }
        found_forbidden = sorted(k for k in forbidden_credential_keys if k in data)
        if found_forbidden:
            joined = ", ".join(found_forbidden)
            raise ValueError(
                "Inline credential payload is not supported in Bindings. "
                f"Remove keys: {joined}. Resolve credentials externally at runtime."
            )
        if (
            "file_connectors" in data
            or "table_connectors" in data
            or "sparql_connectors" in data
        ):
            data = copy.deepcopy(data)
            for key in ("file_connectors", "table_connectors", "sparql_connectors"):
                if key in data and isinstance(data[key], dict):
                    for name, val in data[key].items():
                        if isinstance(val, dict) and "__tag__" in val:
                            data[key][name] = {
                                k: v for k, v in val.items() if k != "__tag__"
                            }
            return cls.model_validate(data)

        legacy_patterns_data = data.get("patterns", {})
        data_copy = {k: v for k, v in data.items() if k != "patterns"}
        instance = cls.model_validate(data_copy)

        for connector_name, raw in legacy_patterns_data.items():
            if raw is None:
                continue
            connector_dict = {k: v for k, v in raw.items() if k != "__tag__"}
            tag_val = raw.get("__tag__") if isinstance(raw, dict) else None
            if tag_val == "file":
                instance.file_connectors[connector_name] = FileConnector.model_validate(
                    connector_dict
                )
            elif tag_val == "table":
                instance.table_connectors[connector_name] = (
                    TableConnector.model_validate(connector_dict)
                )
            elif tag_val == "sparql":
                instance.sparql_connectors[connector_name] = (
                    SparqlConnector.model_validate(connector_dict)
                )
            else:
                if "table_name" in connector_dict:
                    instance.table_connectors[connector_name] = (
                        TableConnector.model_validate(connector_dict)
                    )
                elif "regex" in connector_dict or "sub_path" in connector_dict:
                    instance.file_connectors[connector_name] = (
                        FileConnector.model_validate(connector_dict)
                    )
                elif "rdf_class" in connector_dict:
                    instance.sparql_connectors[connector_name] = (
                        SparqlConnector.model_validate(connector_dict)
                    )
                else:
                    raise ValueError(
                        f"Unable to determine connector type for '{connector_name}'. "
                        "Expected '__tag__: file|table|sparql', "
                        "or connector fields (table_name for TableConnector, "
                        "regex/sub_path for FileConnector, "
                        "rdf_class for SparqlConnector)"
                    )
        return instance

    def add_file_connector(self, name: str, file_connector: FileConnector):
        """Add a file connector to the collection.

        Args:
            name: Name of the connector
            file_connector: FileConnector instance
        """
        self.file_connectors[name] = file_connector

    def add_table_connector(self, name: str, table_connector: TableConnector):
        """Add a table connector to the collection.

        Args:
            name: Name of the connector
            table_connector: TableConnector instance
        """
        self.table_connectors[name] = table_connector

    def add_sparql_connector(self, name: str, sparql_connector: SparqlConnector):
        """Add a SPARQL connector to the collection.

        Args:
            name: Name of the connector (typically the rdf:Class local name)
            sparql_connector: SparqlConnector instance
        """
        self.sparql_connectors[name] = sparql_connector

    def get_sparql_config(self, resource_name: str) -> Any:
        """Get SPARQL endpoint config for a resource.

        Args:
            resource_name: Name of the resource

        Returns:
            SparqlEndpointConfig if resource is a SPARQL connector, None otherwise
        """
        if resource_name in self.sparql_connectors:
            connector = self.sparql_connectors[resource_name]
            if connector.endpoint_url:
                for cfg in self.sparql_configs.values():
                    if (
                        hasattr(cfg, "query_endpoint")
                        and cfg.query_endpoint == connector.endpoint_url
                    ):
                        return cfg
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
        if resource_name in self.file_connectors:
            return self.file_connectors[resource_name].get_resource_type()
        if resource_name in self.table_connectors:
            return self.table_connectors[resource_name].get_resource_type()
        if resource_name in self.sparql_connectors:
            return self.sparql_connectors[resource_name].get_resource_type()
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
