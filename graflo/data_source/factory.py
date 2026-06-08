"""Factory for creating data source instances.

This module provides a factory for creating appropriate data source instances
based on configuration. API sources are built via bindings and RegistryBuilder;
this factory covers file, SQL, and in-memory sources.
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from graflo.architecture.graph_types import EncodingType
from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.data_source.file import (
    JsonFileDataSource,
    JsonlFileDataSource,
    ParquetFileDataSource,
    TableFileDataSource,
)
from graflo.data_source.memory import InMemoryDataSource
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.util.chunker import ChunkerFactory, ChunkerType

logger = logging.getLogger(__name__)


class DataSourceFactory:
    """Factory for creating data source instances."""

    @staticmethod
    def _guess_file_type(filename: Path) -> ChunkerType:
        return ChunkerFactory._guess_chunker_type(filename)

    @classmethod
    def create_file_data_source(
        cls,
        path: Path | str,
        file_type: str | ChunkerType | None = None,
        encoding: EncodingType = EncodingType.UTF_8,
        sep: str | None = None,
    ) -> (
        JsonFileDataSource
        | JsonlFileDataSource
        | TableFileDataSource
        | ParquetFileDataSource
    ):
        if isinstance(path, str):
            path = Path(path)

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

        if file_type_enum == ChunkerType.JSON:
            return JsonFileDataSource(path=path, encoding=encoding)
        if file_type_enum == ChunkerType.JSONL:
            return JsonlFileDataSource(path=path, encoding=encoding)
        if file_type_enum == ChunkerType.TABLE:
            return TableFileDataSource(path=path, encoding=encoding, sep=sep or ",")
        if file_type_enum == ChunkerType.PARQUET:
            return ParquetFileDataSource(path=path)
        raise ValueError(f"Unsupported file type: {file_type_enum}")

    @classmethod
    def create_sql_data_source(cls, config: SQLConfig) -> SQLDataSource:
        return SQLDataSource(config=config)

    @classmethod
    def create_in_memory_data_source(
        cls,
        data: list[dict] | list[list] | pd.DataFrame,
        columns: list[str] | None = None,
    ) -> InMemoryDataSource:
        return InMemoryDataSource(data=data, columns=columns)

    @classmethod
    def create_data_source(
        cls,
        source_type: DataSourceType | str | None = None,
        **kwargs: Any,
    ) -> AbstractDataSource:
        if source_type is None:
            if "path" in kwargs or "file_type" in kwargs:
                source_type = DataSourceType.FILE
            elif "data" in kwargs:
                source_type = DataSourceType.IN_MEMORY
            elif "config" in kwargs:
                config = kwargs["config"]
                if isinstance(config, dict):
                    if "connection_string" in config or "query" in config:
                        source_type = DataSourceType.SQL
                    elif "source_type" in config:
                        source_type = DataSourceType(config["source_type"].lower())
                    else:
                        raise ValueError(
                            "Cannot determine source type from config. "
                            "Please specify source_type or provide "
                            "'connection_string'/'query' (SQL) in config."
                        )
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
                    "provide one of: path (FILE), data (IN_MEMORY), or config (SQL)."
                )

        if isinstance(source_type, str):
            source_type = DataSourceType(source_type.lower())

        if source_type == DataSourceType.API:
            raise ValueError(
                "API data sources must be declared via bindings (APIConnector) and "
                "built with RegistryBuilder; inline API factory creation is not supported."
            )

        if source_type == DataSourceType.FILE:
            return cls.create_file_data_source(**kwargs)
        if source_type == DataSourceType.SQL:
            if "config" not in kwargs:
                config = SQLConfig.from_dict(kwargs)
                return cls.create_sql_data_source(config=config)
            config = kwargs["config"]
            if isinstance(config, dict):
                config = SQLConfig.from_dict(config)
            return cls.create_sql_data_source(config=config)
        if source_type == DataSourceType.IN_MEMORY:
            if "data" not in kwargs:
                raise ValueError("In-memory data source requires 'data' parameter")
            return cls.create_in_memory_data_source(**kwargs)
        raise ValueError(f"Unsupported data source type: {source_type}")

    @classmethod
    def create_data_source_from_config(
        cls, config: dict[str, Any]
    ) -> AbstractDataSource:
        config = config.copy()
        source_type = config.pop("source_type", None)
        if source_type is not None and str(source_type).lower() == "api":
            raise ValueError(
                "API data sources must be declared via bindings (APIConnector) and "
                "ingested through GraphEngine.define_and_ingest with a ConnectionProvider."
            )
        return cls.create_data_source(source_type=source_type, **config)
