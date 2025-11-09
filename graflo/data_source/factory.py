"""Factory for creating data source instances.

This module provides a factory for creating appropriate data source instances
based on configuration. It supports file-based, API, and SQL data sources.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from graflo.architecture.onto import EncodingType
from graflo.data_source.api import APIConfig, APIDataSource
from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.data_source.file import (
    JsonFileDataSource,
    JsonlFileDataSource,
    TableFileDataSource,
)
from graflo.data_source.memory import InMemoryDataSource
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.util.chunker import ChunkerFactory, ChunkerType

logger = logging.getLogger(__name__)


class DataSourceFactory:
    """Factory for creating data source instances.

    This factory creates appropriate data source instances based on the
    provided configuration. It supports file-based, API, and SQL data sources.
    """

    @staticmethod
    def _guess_file_type(filename: Path) -> ChunkerType:
        """Guess the file type based on file extension.

        Args:
            filename: Path to the file

        Returns:
            ChunkerType: Guessed file type

        Raises:
            ValueError: If file extension is not recognized
        """
        return ChunkerFactory._guess_chunker_type(filename)

    @classmethod
    def create_file_data_source(
        cls,
        path: Path | str,
        file_type: str | ChunkerType | None = None,
        encoding: EncodingType = EncodingType.UTF_8,
        sep: str | None = None,
    ) -> JsonFileDataSource | JsonlFileDataSource | TableFileDataSource:
        """Create a file-based data source.

        Args:
            path: Path to the file
            file_type: Type of file ('json', 'jsonl', 'table') or ChunkerType.
                If None, will be guessed from file extension.
            encoding: File encoding (default: UTF_8)
            sep: Field separator for table files (default: ',').
                Only used for table files.

        Returns:
            Appropriate file data source instance (JsonFileDataSource,
            JsonlFileDataSource, or TableFileDataSource)

        Raises:
            ValueError: If file type cannot be determined
        """
        if isinstance(path, str):
            path = Path(path)

        # Determine file type
        if file_type is None:
            try:
                file_type_enum = cls._guess_file_type(path)
            except ValueError as e:
                raise ValueError(
                    f"Could not determine file type for {path}. "
                    f"Please specify file_type explicitly. Error: {e}"
                )
        elif isinstance(file_type, str):
            file_type_enum = ChunkerType(file_type.lower())
        else:
            file_type_enum = file_type

        # Create appropriate data source
        if file_type_enum == ChunkerType.JSON:
            return JsonFileDataSource(path=path, encoding=encoding)
        elif file_type_enum == ChunkerType.JSONL:
            return JsonlFileDataSource(path=path, encoding=encoding)
        elif file_type_enum == ChunkerType.TABLE:
            # sep is only for table files
            return TableFileDataSource(path=path, encoding=encoding, sep=sep or ",")
        else:
            raise ValueError(f"Unsupported file type: {file_type_enum}")

    @classmethod
    def create_api_data_source(cls, config: APIConfig) -> APIDataSource:
        """Create an API data source.

        Args:
            config: API configuration

        Returns:
            APIDataSource instance
        """
        return APIDataSource(config=config)

    @classmethod
    def create_sql_data_source(cls, config: SQLConfig) -> SQLDataSource:
        """Create a SQL data source.

        Args:
            config: SQL configuration

        Returns:
            SQLDataSource instance
        """
        return SQLDataSource(config=config)

    @classmethod
    def create_in_memory_data_source(
        cls,
        data: list[dict] | list[list] | pd.DataFrame,
        columns: list[str] | None = None,
    ) -> InMemoryDataSource:
        """Create an in-memory data source.

        Args:
            data: Data to process (list[dict], list[list], or pd.DataFrame)
            columns: Optional column names for list[list] data

        Returns:
            InMemoryDataSource instance
        """
        return InMemoryDataSource(data=data, columns=columns)

    @classmethod
    def create_data_source(
        cls,
        source_type: DataSourceType | str | None = None,
        **kwargs: Any,
    ) -> AbstractDataSource:
        """Create a data source of the specified type.

        This is a general factory method that routes to specific factory methods
        based on the source type.

        Args:
            source_type: Type of data source to create. If None, will be inferred
                from kwargs (e.g., 'path' -> FILE, 'data' -> IN_MEMORY, 'config' with url -> API)
            **kwargs: Configuration parameters for the data source

        Returns:
            Data source instance

        Raises:
            ValueError: If source type is not supported or required parameters are missing
        """
        # Auto-detect source type if not provided
        if source_type is None:
            if "path" in kwargs or "file_type" in kwargs:
                source_type = DataSourceType.FILE
            elif "data" in kwargs:
                source_type = DataSourceType.IN_MEMORY
            elif "config" in kwargs:
                config = kwargs["config"]
                # Check if it's an API config (has 'url') or SQL config (has 'connection_string')
                if isinstance(config, dict):
                    if "url" in config:
                        source_type = DataSourceType.API
                    elif "connection_string" in config or "query" in config:
                        source_type = DataSourceType.SQL
                    else:
                        # Try to create from dict
                        if "source_type" in config:
                            source_type = DataSourceType(config["source_type"].lower())
                        else:
                            raise ValueError(
                                "Cannot determine source type from config. "
                                "Please specify source_type or provide 'url' (API) "
                                "or 'connection_string'/'query' (SQL) in config."
                            )
                elif hasattr(config, "url"):
                    source_type = DataSourceType.API
                elif hasattr(config, "connection_string") or hasattr(config, "query"):
                    source_type = DataSourceType.SQL
                else:
                    raise ValueError(
                        "Cannot determine source type from config. "
                        "Please specify source_type explicitly."
                    )
            else:
                raise ValueError(
                    "Cannot determine source type. Please specify source_type or "
                    "provide one of: path (FILE), data (IN_MEMORY), or config (API/SQL)."
                )

        if isinstance(source_type, str):
            source_type = DataSourceType(source_type.lower())

        if source_type == DataSourceType.FILE:
            return cls.create_file_data_source(**kwargs)
        elif source_type == DataSourceType.API:
            if "config" not in kwargs:
                # Create APIConfig from kwargs
                from graflo.data_source.api import APIConfig, PaginationConfig

                # Handle nested pagination config manually
                api_kwargs = kwargs.copy()
                pagination_dict = api_kwargs.pop("pagination", None)
                pagination = None
                if pagination_dict is not None:
                    if isinstance(pagination_dict, dict):
                        # Manually construct PaginationConfig to avoid dataclass_wizard issues
                        pagination = PaginationConfig(**pagination_dict)
                    else:
                        pagination = pagination_dict
                api_kwargs["pagination"] = pagination
                config = APIConfig(**api_kwargs)
                return cls.create_api_data_source(config=config)
            config = kwargs["config"]
            if isinstance(config, dict):
                from graflo.data_source.api import APIConfig, PaginationConfig

                # Handle nested pagination config manually
                config_copy = config.copy()
                pagination_dict = config_copy.pop("pagination", None)
                pagination = None
                if pagination_dict is not None:
                    if isinstance(pagination_dict, dict):
                        # Manually construct PaginationConfig to avoid dataclass_wizard issues
                        pagination = PaginationConfig(**pagination_dict)
                    else:
                        pagination = pagination_dict
                config_copy["pagination"] = pagination
                config = APIConfig(**config_copy)
            return cls.create_api_data_source(config=config)
        elif source_type == DataSourceType.SQL:
            if "config" not in kwargs:
                # Create SQLConfig from kwargs
                from graflo.data_source.sql import SQLConfig

                config = SQLConfig.from_dict(kwargs)
                return cls.create_sql_data_source(config=config)
            config = kwargs["config"]
            if isinstance(config, dict):
                from graflo.data_source.sql import SQLConfig

                config = SQLConfig.from_dict(config)
            return cls.create_sql_data_source(config=config)
        elif source_type == DataSourceType.IN_MEMORY:
            if "data" not in kwargs:
                raise ValueError("In-memory data source requires 'data' parameter")
            return cls.create_in_memory_data_source(**kwargs)
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")

    @classmethod
    def create_data_source_from_config(
        cls, config: dict[str, Any]
    ) -> AbstractDataSource:
        """Create a data source from a configuration dictionary.

        The configuration dict should contain:
        - 'source_type': Type of data source (FILE, API, SQL, IN_MEMORY)
        - Other parameters specific to the data source type

        Examples:
            File source:
                {"source_type": "file", "path": "data.json"}
            API source:
                {"source_type": "api", "config": {"url": "https://api.example.com"}}
            SQL source:
                {"source_type": "sql", "config": {"connection_string": "...", "query": "..."}}
            In-memory source:
                {"source_type": "in_memory", "data": [...]}

        Args:
            config: Configuration dictionary

        Returns:
            Data source instance

        Raises:
            ValueError: If configuration is invalid
        """
        config = config.copy()
        source_type = config.pop("source_type", None)
        return cls.create_data_source(source_type=source_type, **config)
