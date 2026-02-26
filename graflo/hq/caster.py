"""Data casting and ingestion system for graph databases.

This module provides functionality for casting and ingesting data into graph databases.
It handles batch processing, file discovery, and database operations for both ArangoDB
and Neo4j.

Key Components:
    - Caster: Main class for data casting and ingestion
    - FilePattern: Pattern matching for file discovery
    - Patterns: Collection of file patterns for different resources

Example:
    >>> caster = Caster(schema=schema)
    >>> caster.ingest(path="data/", conn_conf=db_config)
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, cast

import pandas as pd
from pydantic import BaseModel

from suthing import Timer

from graflo.architecture.onto import EncodingType, GraphContainer
from graflo.architecture.schema import Schema
from graflo.data_source import (
    AbstractDataSource,
    DataSourceFactory,
    DataSourceRegistry,
)
from graflo.db import DBConfig
from graflo.hq.db_writer import DBWriter
from graflo.hq.registry_builder import RegistryBuilder
from graflo.util.chunker import ChunkerType
from graflo.util.onto import Patterns

logger = logging.getLogger(__name__)


class IngestionParams(BaseModel):
    """Parameters for controlling the ingestion process.

    Attributes:
        clear_data: If True, remove all existing graph data before ingestion without
            changing the schema.
        n_cores: Number of CPU cores/threads to use for parallel processing
        max_items: Maximum number of items to process per resource (applies to all data sources)
        batch_size: Size of batches for processing
        dry: Whether to perform a dry run (no database changes)
        init_only: Whether to only initialize the database without ingestion
        limit_files: Optional limit on number of files to process
        max_concurrent_db_ops: Maximum number of concurrent database operations (for vertices/edges).
            If None, uses n_cores. Set to 1 to prevent deadlocks in databases that don't handle
            concurrent transactions well (e.g., Neo4j). Database-independent setting.
        datetime_after: Inclusive lower bound for datetime filtering (ISO format).
            Rows with date_column >= datetime_after are included. Used with SQL/table sources.
        datetime_before: Exclusive upper bound for datetime filtering (ISO format).
            Rows with date_column < datetime_before are included. Range is [datetime_after, datetime_before).
        datetime_column: Default column name for datetime filtering when the pattern does not
            specify date_field. Per-table override: set date_field on TablePattern (or FilePattern).
    """

    clear_data: bool = False
    n_cores: int = 1
    max_items: int | None = None
    batch_size: int = 10000
    dry: bool = False
    init_only: bool = False
    limit_files: int | None = None
    max_concurrent_db_ops: int | None = None
    datetime_after: str | None = None
    datetime_before: str | None = None
    datetime_column: str | None = None


class Caster:
    """Main class for data casting and ingestion.

    This class handles the process of casting data into graph structures and
    ingesting them into the database. It supports batch processing, parallel
    execution, and various data formats.

    Attributes:
        schema: Schema configuration for the graph
        ingestion_params: IngestionParams instance controlling ingestion behavior
    """

    def __init__(
        self,
        schema: Schema,
        ingestion_params: IngestionParams | None = None,
        **kwargs,
    ):
        """Initialize the caster with schema and configuration.

        Args:
            schema: Schema configuration for the graph
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, creates IngestionParams from kwargs or uses defaults
            **kwargs: Additional configuration options (for backward compatibility):
                - clear_data: Whether to clear existing data before ingestion
                - n_cores: Number of CPU cores/threads to use for parallel processing
                - max_items: Maximum number of items to process
                - batch_size: Size of batches for processing
                - dry: Whether to perform a dry run
        """
        if ingestion_params is None:
            ingestion_params = IngestionParams(**kwargs)
        self.ingestion_params = ingestion_params
        self.schema = schema

    # ------------------------------------------------------------------
    # Casting
    # ------------------------------------------------------------------

    async def cast_normal_resource(
        self, data, resource_name: str | None = None
    ) -> GraphContainer:
        """Cast data into a graph container using a resource.

        Args:
            data: Data to cast
            resource_name: Optional name of the resource to use

        Returns:
            GraphContainer: Container with cast graph data
        """
        rr = self.schema.fetch_resource(resource_name)

        semaphore = asyncio.Semaphore(self.ingestion_params.n_cores)

        async def process_doc(doc):
            async with semaphore:
                return await asyncio.to_thread(rr, doc)

        docs = await asyncio.gather(*[process_doc(doc) for doc in data])

        graph = GraphContainer.from_docs_list(docs)
        return graph

    # ------------------------------------------------------------------
    # Processing pipeline
    # ------------------------------------------------------------------

    async def process_batch(
        self,
        batch,
        resource_name: str | None,
        conn_conf: None | DBConfig = None,
    ):
        """Process a batch of data.

        Args:
            batch: Batch of data to process
            resource_name: Optional name of the resource to use
            conn_conf: Optional database connection configuration
        """
        gc = await self.cast_normal_resource(batch, resource_name=resource_name)

        if conn_conf is not None:
            writer = self._make_db_writer()
            await writer.write(gc=gc, conn_conf=conn_conf, resource_name=resource_name)

    async def process_data_source(
        self,
        data_source: AbstractDataSource,
        resource_name: str | None = None,
        conn_conf: None | DBConfig = None,
    ):
        """Process a data source.

        Args:
            data_source: Data source to process
            resource_name: Optional name of the resource (overrides data_source.resource_name)
            conn_conf: Optional database connection configuration
        """
        actual_resource_name = resource_name or data_source.resource_name

        limit = getattr(data_source, "_pattern_limit", None)
        if limit is None:
            limit = self.ingestion_params.max_items

        for batch in data_source.iter_batches(
            batch_size=self.ingestion_params.batch_size, limit=limit
        ):
            await self.process_batch(
                batch, resource_name=actual_resource_name, conn_conf=conn_conf
            )

    async def process_resource(
        self,
        resource_instance: (
            Path | str | list[dict] | list[list] | pd.DataFrame | dict[str, Any]
        ),
        resource_name: str | None,
        conn_conf: None | DBConfig = None,
        **kwargs,
    ):
        """Process a resource instance from configuration or direct data.

        This method accepts either:
        1. A configuration dictionary with 'source_type' and data source parameters
        2. A file path (Path or str) - creates FileDataSource
        3. In-memory data (list[dict], list[list], or pd.DataFrame) - creates InMemoryDataSource

        Args:
            resource_instance: Configuration dict, file path, or in-memory data.
                Configuration dict format:
                - {"source_type": "file", "path": "data.json"}
                - {"source_type": "api", "config": {"url": "https://..."}}
                - {"source_type": "sql", "config": {"connection_string": "...", "query": "..."}}
                - {"source_type": "in_memory", "data": [...]}
            resource_name: Optional name of the resource
            conn_conf: Optional database connection configuration
            **kwargs: Additional arguments passed to data source creation
                (e.g., columns for list[list], encoding for files)
        """
        if isinstance(resource_instance, dict):
            config = resource_instance.copy()
            config.update(kwargs)
            data_source = DataSourceFactory.create_data_source_from_config(config)
        elif isinstance(resource_instance, (Path, str)):
            file_type: str | ChunkerType | None = cast(
                str | ChunkerType | None, kwargs.get("file_type", None)
            )
            encoding: EncodingType = cast(
                EncodingType, kwargs.get("encoding", EncodingType.UTF_8)
            )
            sep: str | None = cast(str | None, kwargs.get("sep", None))
            data_source = DataSourceFactory.create_file_data_source(
                path=resource_instance,
                file_type=file_type,
                encoding=encoding,
                sep=sep,
            )
        else:
            columns: list[str] | None = cast(
                list[str] | None, kwargs.get("columns", None)
            )
            data_source = DataSourceFactory.create_in_memory_data_source(
                data=resource_instance,
                columns=columns,
            )

        data_source.resource_name = resource_name

        await self.process_data_source(
            data_source=data_source,
            resource_name=resource_name,
            conn_conf=conn_conf,
        )

    # ------------------------------------------------------------------
    # Queue-based processing
    # ------------------------------------------------------------------

    async def process_with_queue(
        self, tasks: asyncio.Queue, conn_conf: DBConfig | None = None
    ):
        """Process tasks from a queue.

        Args:
            tasks: Async queue of tasks to process
            conn_conf: Optional database connection configuration
        """
        SENTINEL = None

        while True:
            try:
                task = await tasks.get()

                if task is SENTINEL:
                    tasks.task_done()
                    break

                if isinstance(task, tuple) and len(task) == 2:
                    filepath, resource_name = task
                    await self.process_resource(
                        resource_instance=filepath,
                        resource_name=resource_name,
                        conn_conf=conn_conf,
                    )
                elif isinstance(task, AbstractDataSource):
                    await self.process_data_source(
                        data_source=task, conn_conf=conn_conf
                    )
                tasks.task_done()
            except Exception as e:
                logger.error(f"Error processing task: {e}", exc_info=True)
                tasks.task_done()
                break

    # ------------------------------------------------------------------
    # Normalization utility
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_resource(
        data: pd.DataFrame | list[list] | list[dict], columns: list[str] | None = None
    ) -> list[dict]:
        """Normalize resource data into a list of dictionaries.

        Args:
            data: Data to normalize (DataFrame, list of lists, or list of dicts)
            columns: Optional column names for list data

        Returns:
            list[dict]: Normalized data as list of dictionaries

        Raises:
            ValueError: If columns is not provided for list data
        """
        if isinstance(data, pd.DataFrame):
            columns = data.columns.tolist()
            _data = data.values.tolist()
        elif data and isinstance(data[0], list):
            _data = cast(list[list], data)
            if columns is None:
                raise ValueError("columns should be set")
        else:
            return cast(list[dict], data)
        rows_dressed = [{k: v for k, v in zip(columns, item)} for item in _data]
        return rows_dressed

    # ------------------------------------------------------------------
    # Ingestion orchestration
    # ------------------------------------------------------------------

    async def ingest_data_sources(
        self,
        data_source_registry: DataSourceRegistry,
        conn_conf: DBConfig,
        ingestion_params: IngestionParams | None = None,
    ):
        """Ingest data from data sources in a registry.

        Note: Schema definition should be handled separately via GraphEngine.define_schema()
        before calling this method.

        Args:
            data_source_registry: Registry containing data sources mapped to resources
            conn_conf: Database connection configuration
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        if ingestion_params is None:
            ingestion_params = IngestionParams()

        self.ingestion_params = ingestion_params
        init_only = ingestion_params.init_only

        if init_only:
            logger.info("ingest execution bound to init")
            sys.exit(0)

        tasks: list[AbstractDataSource] = []
        for resource_name in self.schema._resources.keys():
            data_sources = data_source_registry.get_data_sources(resource_name)
            if data_sources:
                logger.info(
                    f"For resource name {resource_name} {len(data_sources)} data sources were found"
                )
                tasks.extend(data_sources)

        with Timer() as klepsidra:
            if self.ingestion_params.n_cores > 1:
                queue_tasks: asyncio.Queue = asyncio.Queue()
                for item in tasks:
                    await queue_tasks.put(item)

                for _ in range(self.ingestion_params.n_cores):
                    await queue_tasks.put(None)

                worker_tasks = [
                    self.process_with_queue(queue_tasks, conn_conf=conn_conf)
                    for _ in range(self.ingestion_params.n_cores)
                ]

                await asyncio.gather(*worker_tasks)
            else:
                for data_source in tasks:
                    await self.process_data_source(
                        data_source=data_source, conn_conf=conn_conf
                    )
        logger.info(f"Processing took {klepsidra.elapsed:.1f} sec")

    def ingest(
        self,
        target_db_config: DBConfig,
        patterns: Patterns | None = None,
        ingestion_params: IngestionParams | None = None,
    ):
        """Ingest data into the graph database.

        This is the main ingestion method that takes:
        - Schema: Graph structure (already set in Caster)
        - OutputConfig: Target graph database configuration
        - Patterns: Mapping of resources to physical data sources
        - IngestionParams: Parameters controlling the ingestion process

        Args:
            target_db_config: Target database connection configuration (for writing graph)
            patterns: Patterns instance mapping resources to data sources
                If None, defaults to empty Patterns()
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        patterns = patterns or Patterns()
        ingestion_params = ingestion_params or IngestionParams()

        db_flavor = target_db_config.connection_type
        self.schema.database_features.db_flavor = db_flavor
        self.schema.finish_init()

        registry = RegistryBuilder(self.schema).build(patterns, ingestion_params)

        asyncio.run(
            self.ingest_data_sources(
                data_source_registry=registry,
                conn_conf=target_db_config,
                ingestion_params=ingestion_params,
            )
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_db_writer(self) -> DBWriter:
        """Create a :class:`DBWriter` from the current ingestion params."""
        max_concurrent = (
            self.ingestion_params.max_concurrent_db_ops
            if self.ingestion_params.max_concurrent_db_ops is not None
            else self.ingestion_params.n_cores
        )
        return DBWriter(
            schema=self.schema,
            dry=self.ingestion_params.dry,
            max_concurrent=max_concurrent,
        )
