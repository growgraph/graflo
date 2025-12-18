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

import logging
import multiprocessing as mp
import queue
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
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
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.db import DBType, ConnectionManager, DBConfig
from graflo.onto import DBFlavor
from graflo.util.chunker import ChunkerType
from graflo.util.onto import FilePattern, Patterns, ResourceType, TablePattern

logger = logging.getLogger(__name__)


class IngestionParams(BaseModel):
    """Parameters for controlling the ingestion process.

    Attributes:
        clean_start: Whether to clean the database before ingestion
        n_cores: Number of CPU cores/threads to use for parallel processing
        max_items: Maximum number of items to process per resource (applies to all data sources)
        batch_size: Size of batches for processing
        dry: Whether to perform a dry run (no database changes)
        init_only: Whether to only initialize the database without ingestion
        limit_files: Optional limit on number of files to process
    """

    clean_start: bool = False
    n_cores: int = 1
    max_items: int | None = None
    batch_size: int = 10000
    dry: bool = False
    init_only: bool = False
    limit_files: int | None = None


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
                - clean_start: Whether to clean the database before ingestion
                - n_cores: Number of CPU cores/threads to use for parallel processing
                - max_items: Maximum number of items to process
                - batch_size: Size of batches for processing
                - dry: Whether to perform a dry run
        """
        if ingestion_params is None:
            # Create IngestionParams from kwargs or use defaults
            ingestion_params = IngestionParams(**kwargs)
        self.ingestion_params = ingestion_params
        self.schema = schema

    @staticmethod
    def discover_files(
        fpath: Path | str, pattern: FilePattern, limit_files=None
    ) -> list[Path]:
        """Discover files matching a pattern in a directory.

        Args:
            fpath: Path to search in (should be the directory containing files)
            pattern: Pattern to match files against
            limit_files: Optional limit on number of files to return

        Returns:
            list[Path]: List of matching file paths

        Raises:
            AssertionError: If pattern.sub_path is None
        """
        assert pattern.sub_path is not None
        if isinstance(fpath, str):
            fpath_pathlib = Path(fpath)
        else:
            fpath_pathlib = fpath

        # fpath is already the directory to search (pattern.sub_path from caller)
        # so we use it directly, not combined with pattern.sub_path again
        files = [
            f
            for f in fpath_pathlib.iterdir()
            if f.is_file()
            and (
                True
                if pattern.regex is None
                else re.search(pattern.regex, f.name) is not None
            )
        ]

        if limit_files is not None:
            files = files[:limit_files]

        return files

    def cast_normal_resource(
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

        with ThreadPoolExecutor(max_workers=self.ingestion_params.n_cores) as executor:
            docs = list(
                executor.map(
                    lambda doc: rr(doc),
                    data,
                )
            )

        graph = GraphContainer.from_docs_list(docs)
        return graph

    def process_batch(
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
        gc = self.cast_normal_resource(batch, resource_name=resource_name)

        if conn_conf is not None:
            self.push_db(gc=gc, conn_conf=conn_conf, resource_name=resource_name)

    def process_data_source(
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
        # Use provided resource_name or fall back to data_source's resource_name
        actual_resource_name = resource_name or data_source.resource_name

        # Use pattern-specific limit if available, otherwise use global max_items
        limit = getattr(data_source, "_pattern_limit", None)
        if limit is None:
            limit = self.ingestion_params.max_items

        for batch in data_source.iter_batches(
            batch_size=self.ingestion_params.batch_size, limit=limit
        ):
            self.process_batch(
                batch, resource_name=actual_resource_name, conn_conf=conn_conf
            )

    def process_resource(
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
        # Handle configuration dictionary
        if isinstance(resource_instance, dict):
            config = resource_instance.copy()
            # Merge with kwargs (kwargs take precedence)
            config.update(kwargs)
            data_source = DataSourceFactory.create_data_source_from_config(config)
        # Handle file paths
        elif isinstance(resource_instance, (Path, str)):
            # File path - create FileDataSource
            # Extract only valid file data source parameters with proper typing
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
        # Handle in-memory data
        else:
            # In-memory data - create InMemoryDataSource
            # Extract only valid in-memory data source parameters with proper typing
            columns: list[str] | None = cast(
                list[str] | None, kwargs.get("columns", None)
            )
            data_source = DataSourceFactory.create_in_memory_data_source(
                data=resource_instance,
                columns=columns,
            )

        data_source.resource_name = resource_name

        # Process using the data source
        self.process_data_source(
            data_source=data_source,
            resource_name=resource_name,
            conn_conf=conn_conf,
        )

    def push_db(
        self,
        gc: GraphContainer,
        conn_conf: DBConfig,
        resource_name: str | None,
    ):
        """Push graph container data to the database.

        Args:
            gc: Graph container with data to push
            conn_conf: Database connection configuration
            resource_name: Optional name of the resource
        """
        vc = self.schema.vertex_config
        resource = self.schema.fetch_resource(resource_name)
        with ConnectionManager(connection_config=conn_conf) as db_client:
            for vcol, data in gc.vertices.items():
                # blank nodes: push and get back their keys  {"_key": ...}
                if vcol in vc.blank_vertices:
                    query0 = db_client.insert_return_batch(data, vc.vertex_dbname(vcol))
                    cursor = db_client.execute(query0)
                    gc.vertices[vcol] = [item for item in cursor]
                else:
                    db_client.upsert_docs_batch(
                        data,
                        vc.vertex_dbname(vcol),
                        vc.index(vcol),
                        update_keys="doc",
                        filter_uniques=True,
                        dry=self.ingestion_params.dry,
                    )

            # update edge misc with blank node edges
            for vcol in vc.blank_vertices:
                for edge_id, edge in self.schema.edge_config.edges_items():
                    vfrom, vto, relation = edge_id
                    if vcol == vfrom or vcol == vto:
                        if edge_id not in gc.edges:
                            gc.edges[edge_id] = []
                        gc.edges[edge_id].extend(
                            [
                                (x, y, {})
                                for x, y in zip(gc.vertices[vfrom], gc.vertices[vto])
                            ]
                        )

        with ConnectionManager(connection_config=conn_conf) as db_client:
            # currently works only on item level
            for edge in resource.extra_weights:
                if edge.weights is None:
                    continue
                for weight in edge.weights.vertices:
                    if weight.name in vc.vertex_set:
                        index_fields = vc.index(weight.name)

                        if not self.ingestion_params.dry and weight.name in gc.vertices:
                            weights_per_item = db_client.fetch_present_documents(
                                class_name=vc.vertex_dbname(weight.name),
                                batch=gc.vertices[weight.name],
                                match_keys=index_fields.fields,
                                keep_keys=weight.fields,
                            )

                            for j, item in enumerate(gc.linear):
                                weights = weights_per_item[j]

                                for ee in item[edge.edge_id]:
                                    weight_collection_attached = {
                                        weight.cfield(k): v
                                        for k, v in weights[0].items()
                                    }
                                    ee.update(weight_collection_attached)
                    else:
                        logger.error(f"{weight.name} not a valid vertex")

        with ConnectionManager(connection_config=conn_conf) as db_client:
            for edge_id, edge in self.schema.edge_config.edges_items():
                for ee in gc.loop_over_relations(edge_id):
                    _, _, relation = ee
                    if not self.ingestion_params.dry:
                        data = gc.edges[ee]
                        db_client.insert_edges_batch(
                            docs_edges=data,
                            source_class=vc.vertex_dbname(edge.source),
                            target_class=vc.vertex_dbname(edge.target),
                            relation_name=relation,
                            collection_name=edge.database_name,
                            match_keys_source=vc.index(edge.source).fields,
                            match_keys_target=vc.index(edge.target).fields,
                            filter_uniques=False,
                            dry=self.ingestion_params.dry,
                        )

    def process_with_queue(self, tasks: mp.Queue, conn_conf: DBConfig | None = None):
        """Process tasks from a queue.

        Args:
            tasks: Queue of tasks to process
            conn_conf: Optional database connection configuration
        """
        while True:
            try:
                task = tasks.get_nowait()
                # Support both (Path, str) tuples and DataSource instances
                if isinstance(task, tuple) and len(task) == 2:
                    filepath, resource_name = task
                    self.process_resource(
                        resource_instance=filepath,
                        resource_name=resource_name,
                        conn_conf=conn_conf,
                    )
                elif isinstance(task, AbstractDataSource):
                    self.process_data_source(data_source=task, conn_conf=conn_conf)
            except queue.Empty:
                break

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
            _data = cast(list[list], data)  # Tell mypy this is list[list]
            if columns is None:
                raise ValueError("columns should be set")
        else:
            return cast(list[dict], data)  # Tell mypy this is list[dict]
        rows_dressed = [{k: v for k, v in zip(columns, item)} for item in _data]
        return rows_dressed

    def ingest_data_sources(
        self,
        data_source_registry: DataSourceRegistry,
        conn_conf: DBConfig,
        ingestion_params: IngestionParams | None = None,
    ):
        """Ingest data from data sources in a registry.

        Args:
            data_source_registry: Registry containing data sources mapped to resources
            conn_conf: Database connection configuration
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        if ingestion_params is None:
            ingestion_params = IngestionParams()

        # Update ingestion params (may override defaults set in __init__)
        self.ingestion_params = ingestion_params
        init_only = ingestion_params.init_only

        # If effective_schema is not set, use schema.general.name as fallback
        if conn_conf.can_be_target() and conn_conf.effective_schema is None:
            schema_name = self.schema.general.name
            # Map to the appropriate field based on DB type
            if conn_conf.connection_type == DBType.TIGERGRAPH:
                # TigerGraph uses 'schema_name' field
                conn_conf.schema_name = schema_name
            else:
                # ArangoDB, Neo4j use 'database' field (which maps to effective_schema)
                conn_conf.database = schema_name

        # init_db() now handles database/schema creation automatically
        # It checks if the database exists and creates it if needed
        # Uses schema.general.name if database is not set in config
        with ConnectionManager(connection_config=conn_conf) as db_client:
            db_client.init_db(self.schema, self.ingestion_params.clean_start)

        if init_only:
            logger.info("ingest execution bound to init")
            sys.exit(0)

        # Collect all data sources
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
                queue_tasks: mp.Queue = mp.Queue()
                for item in tasks:
                    queue_tasks.put(item)

                func = partial(
                    self.process_with_queue,
                    conn_conf=conn_conf,
                )
                assert mp.get_start_method() == "fork", (
                    "Requires 'forking' operating system"
                )

                processes = []

                for w in range(self.ingestion_params.n_cores):
                    p = mp.Process(target=func, args=(queue_tasks,))
                    processes.append(p)
                    p.start()
                    for p in processes:
                        p.join()
            else:
                for data_source in tasks:
                    self.process_data_source(
                        data_source=data_source, conn_conf=conn_conf
                    )
        logger.info(f"Processing took {klepsidra.elapsed:.1f} sec")

    @staticmethod
    def _get_db_flavor_from_config(output_config: DBConfig) -> DBFlavor:
        """Convert DBConfig connection type to DBFlavor.

        Args:
            output_config: Database configuration

        Returns:
            DBFlavor enum value corresponding to the database type
        """
        db_type = output_config.connection_type
        if db_type == DBType.ARANGO:
            return DBFlavor.ARANGO
        elif db_type == DBType.NEO4J:
            return DBFlavor.NEO4J
        elif db_type == DBType.TIGERGRAPH:
            return DBFlavor.TIGERGRAPH
        else:
            # Default to ARANGO for unknown types
            return DBFlavor.ARANGO

    def _register_file_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        pattern: FilePattern,
        ingestion_params: IngestionParams,
    ) -> None:
        """Register file data sources for a resource.

        Args:
            registry: Data source registry to add sources to
            resource_name: Name of the resource
            pattern: File pattern configuration
            ingestion_params: Ingestion parameters
        """
        if pattern.sub_path is None:
            logger.warning(
                f"FilePattern for resource '{resource_name}' has no sub_path, skipping"
            )
            return

        path_obj = pattern.sub_path.expanduser()
        files = Caster.discover_files(
            path_obj, limit_files=ingestion_params.limit_files, pattern=pattern
        )
        logger.info(f"For resource name {resource_name} {len(files)} files were found")

        for file_path in files:
            file_source = DataSourceFactory.create_file_data_source(path=file_path)
            registry.register(file_source, resource_name=resource_name)

    def _register_sql_table_sources(
        self,
        registry: DataSourceRegistry,
        resource_name: str,
        pattern: TablePattern,
        patterns: "Patterns",
        ingestion_params: IngestionParams,
    ) -> None:
        """Register SQL table data sources for a resource.

        Uses SQLDataSource with batch processing (cursors) instead of loading
        all data into memory. This is efficient for large tables.

        Args:
            registry: Data source registry to add sources to
            resource_name: Name of the resource
            pattern: Table pattern configuration
            patterns: Patterns instance for accessing configs
            ingestion_params: Ingestion parameters
        """
        postgres_config = patterns.get_postgres_config(resource_name)
        if postgres_config is None:
            logger.warning(
                f"PostgreSQL table '{resource_name}' has no connection config, skipping"
            )
            return

        table_info = patterns.get_table_info(resource_name)
        if table_info is None:
            logger.warning(
                f"Could not get table info for resource '{resource_name}', skipping"
            )
            return

        table_name, schema_name = table_info
        effective_schema = schema_name or postgres_config.schema_name or "public"

        try:
            # Build base query
            query = f'SELECT * FROM "{effective_schema}"."{table_name}"'
            where_clause = pattern.build_where_clause()
            if where_clause:
                query += f" WHERE {where_clause}"

            # Get SQLAlchemy connection string from PostgresConfig
            connection_string = postgres_config.to_sqlalchemy_connection_string()

            # Create SQLDataSource with pagination for efficient batch processing
            # Note: max_items limit is handled by SQLDataSource.iter_batches() limit parameter
            sql_config = SQLConfig(
                connection_string=connection_string,
                query=query,
                pagination=True,
                page_size=ingestion_params.batch_size,  # Use batch_size for page size
            )
            sql_source = SQLDataSource(config=sql_config)

            # Register the SQL data source (it will be processed in batches)
            registry.register(sql_source, resource_name=resource_name)

            logger.info(
                f"Created SQL data source for table '{effective_schema}.{table_name}' "
                f"mapped to resource '{resource_name}' (will process in batches of {ingestion_params.batch_size})"
            )
        except Exception as e:
            logger.error(
                f"Failed to create data source for PostgreSQL table '{resource_name}': {e}",
                exc_info=True,
            )

    def _build_registry_from_patterns(
        self,
        patterns: "Patterns",
        ingestion_params: IngestionParams,
    ) -> DataSourceRegistry:
        """Build data source registry from patterns.

        Args:
            patterns: Patterns instance mapping resources to data sources
            ingestion_params: Ingestion parameters

        Returns:
            DataSourceRegistry with registered data sources
        """
        registry = DataSourceRegistry()

        for resource in self.schema.resources:
            resource_name = resource.name
            resource_type = patterns.get_resource_type(resource_name)

            if resource_type is None:
                logger.warning(
                    f"No resource type found for resource '{resource_name}', skipping"
                )
                continue

            pattern = patterns.patterns.get(resource_name)
            if pattern is None:
                logger.warning(
                    f"No pattern found for resource '{resource_name}', skipping"
                )
                continue

            if resource_type == ResourceType.FILE:
                if not isinstance(pattern, FilePattern):
                    logger.warning(
                        f"Pattern for resource '{resource_name}' is not a FilePattern, skipping"
                    )
                    continue
                self._register_file_sources(
                    registry, resource_name, pattern, ingestion_params
                )

            elif resource_type == ResourceType.SQL_TABLE:
                if not isinstance(pattern, TablePattern):
                    logger.warning(
                        f"Pattern for resource '{resource_name}' is not a TablePattern, skipping"
                    )
                    continue
                self._register_sql_table_sources(
                    registry, resource_name, pattern, patterns, ingestion_params
                )

            else:
                logger.warning(
                    f"Unsupported resource type '{resource_type}' for resource '{resource_name}', skipping"
                )

        return registry

    def ingest(
        self,
        output_config: DBConfig,
        patterns: "Patterns | None" = None,
        ingestion_params: IngestionParams | None = None,
    ):
        """Ingest data into the graph database.

        This is the main ingestion method that takes:
        - Schema: Graph structure (already set in Caster)
        - OutputConfig: Target graph database configuration
        - Patterns: Mapping of resources to physical data sources
        - IngestionParams: Parameters controlling the ingestion process

        Args:
            output_config: Target database connection configuration (for writing graph)
            patterns: Patterns instance mapping resources to data sources
                If None, defaults to empty Patterns()
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        # Normalize parameters
        patterns = patterns or Patterns()
        ingestion_params = ingestion_params or IngestionParams()

        # Initialize vertex config with correct field types based on database type
        db_flavor = self._get_db_flavor_from_config(output_config)
        self.schema.vertex_config.finish_init(db_flavor)
        # Initialize edge config after vertex config is fully initialized
        self.schema.edge_config.finish_init(self.schema.vertex_config)

        # Build registry from patterns
        registry = self._build_registry_from_patterns(patterns, ingestion_params)

        # Ingest data sources
        self.ingest_data_sources(
            data_source_registry=registry,
            conn_conf=output_config,
            ingestion_params=ingestion_params,
        )
