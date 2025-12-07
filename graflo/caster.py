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

from __future__ import annotations

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
from suthing import Timer

from graflo.architecture.onto import EncodingType, GraphContainer
from graflo.architecture.schema import Schema
from graflo.data_source import (
    AbstractDataSource,
    DataSourceFactory,
    DataSourceRegistry,
)
from graflo.db import DBType, ConnectionManager, DBConfig
from graflo.util.chunker import ChunkerType
from graflo.util.onto import FilePattern, Patterns, TablePattern

logger = logging.getLogger(__name__)


class Caster:
    """Main class for data casting and ingestion.

    This class handles the process of casting data into graph structures and
    ingesting them into the database. It supports batch processing, parallel
    execution, and various data formats.

    Attributes:
        clean_start: Whether to clean the database before ingestion
        n_cores: Number of CPU cores to use for parallel processing
        max_items: Maximum number of items to process
        batch_size: Size of batches for processing
        n_threads: Number of threads for parallel processing
        dry: Whether to perform a dry run (no database changes)
        schema: Schema configuration for the graph
    """

    def __init__(self, schema: Schema, **kwargs):
        """Initialize the caster with schema and configuration.

        Args:
            schema: Schema configuration for the graph
            **kwargs: Additional configuration options:
                - clean_start: Whether to clean the database before ingestion
                - n_cores: Number of CPU cores to use
                - max_items: Maximum number of items to process
                - batch_size: Size of batches for processing
                - n_threads: Number of threads for parallel processing
                - dry: Whether to perform a dry run
        """
        self.clean_start: bool = False
        self.n_cores = kwargs.pop("n_cores", 1)
        self.max_items = kwargs.pop("max_items", None)
        self.batch_size = kwargs.pop("batch_size", 10000)
        self.n_threads = kwargs.pop("n_threads", 1)
        self.dry = kwargs.pop("dry", False)
        self.schema = schema

    @staticmethod
    def discover_files(
        fpath: Path | str, pattern: FilePattern, limit_files=None
    ) -> list[Path]:
        """Discover files matching a pattern in a directory.

        Args:
            fpath: Path to search in
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

        files = [
            f
            for f in (fpath_pathlib / pattern.sub_path).iterdir()
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

        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
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

        for batch in data_source.iter_batches(
            batch_size=self.batch_size, limit=self.max_items
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
                        dry=self.dry,
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

                        if not self.dry and weight.name in gc.vertices:
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
                    if not self.dry:
                        data = gc.edges[ee]
                        db_client.insert_edges_batch(
                            docs_edges=data,
                            source_class=vc.vertex_dbname(edge.source),
                            target_class=vc.vertex_dbname(edge.target),
                            relation_name=relation,
                            collection_name=edge.collection_name,
                            match_keys_source=vc.index(edge.source).fields,
                            match_keys_target=vc.index(edge.target).fields,
                            filter_uniques=False,
                            dry=self.dry,
                        )

    def process_with_queue(self, tasks: mp.Queue, **kwargs):
        """Process tasks from a queue.

        Args:
            tasks: Queue of tasks to process
            **kwargs: Additional keyword arguments
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
                        **kwargs,
                    )
                elif isinstance(task, AbstractDataSource):
                    self.process_data_source(data_source=task, **kwargs)
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
        conn_conf: None | DBConfig = None,
        **kwargs,
    ):
        """Ingest data from data sources in a registry.

        Args:
            data_source_registry: Registry containing data sources mapped to resources
            conn_conf: Database connection configuration
            **kwargs: Additional keyword arguments:
                - clean_start: Whether to clean the database before ingestion
                - n_cores: Number of CPU cores to use
                - max_items: Maximum number of items to process
                - batch_size: Size of batches for processing
                - dry: Whether to perform a dry run
                - init_only: Whether to only initialize the database
        """
        conn_conf = cast(DBConfig, kwargs.get("conn_conf", conn_conf))
        self.clean_start = kwargs.pop("clean_start", self.clean_start)
        self.n_cores = kwargs.pop("n_cores", self.n_cores)
        self.max_items = kwargs.pop("max_items", self.max_items)
        self.batch_size = kwargs.pop("batch_size", self.batch_size)
        self.dry = kwargs.pop("dry", self.dry)
        init_only = kwargs.pop("init_only", False)

        if conn_conf is None:
            raise ValueError("conn_conf is required for ingest_data_sources")

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
            db_client.init_db(self.schema, self.clean_start)

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
            if self.n_cores > 1:
                queue_tasks: mp.Queue = mp.Queue()
                for item in tasks:
                    queue_tasks.put(item)

                func = partial(
                    self.process_with_queue,
                    conn_conf=conn_conf,
                    **kwargs,
                )
                assert mp.get_start_method() == "fork", (
                    "Requires 'forking' operating system"
                )

                processes = []

                for w in range(self.n_cores):
                    p = mp.Process(target=func, args=(queue_tasks,), kwargs=kwargs)
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

    def ingest(
        self,
        output_config: DBConfig,
        patterns: "Patterns | None" = None,
        **kwargs,
    ):
        """Ingest data into the graph database.

        This is the main ingestion method that takes:
        - Schema: Graph structure (already set in Caster)
        - OutputConfig: Target graph database configuration
        - Patterns: Mapping of resources to physical data sources

        Args:
            output_config: Target database connection configuration (for writing graph)
            patterns: Patterns instance mapping resources to data sources
                If None, will try to use legacy 'patterns' kwarg
            **kwargs: Additional keyword arguments:
                - clean_start: Whether to clean the database before ingestion
                - n_cores: Number of CPU cores to use
                - max_items: Maximum number of items to process
                - batch_size: Size of batches for processing
                - dry: Whether to perform a dry run
                - init_only: Whether to only initialize the database
                - limit_files: Optional limit on number of files to process
                - conn_conf: Legacy parameter (use output_config instead)
        """
        # Backward compatibility: support legacy conn_conf parameter
        if "conn_conf" in kwargs:
            output_config = kwargs.pop("conn_conf")

        # Backward compatibility: support legacy patterns parameter
        if patterns is None:
            patterns = kwargs.pop("patterns", Patterns())

        # Create DataSourceRegistry from patterns
        registry = DataSourceRegistry()

        for r in self.schema.resources:
            resource_name = r.name
            resource_type = patterns.get_resource_type(resource_name)

            if resource_type == "file":
                # Handle file pattern
                pattern = patterns.patterns[resource_name]
                if not isinstance(pattern, FilePattern):
                    logger.warning(
                        f"Pattern for resource '{resource_name}' is not a FilePattern, skipping"
                    )
                    continue

                # Use sub_path from FilePattern (path is now part of the pattern)
                if pattern.sub_path is None:
                    logger.warning(
                        f"FilePattern for resource '{resource_name}' has no sub_path, skipping"
                    )
                    continue
                path_obj = pattern.sub_path.expanduser()
                limit_files = kwargs.get("limit_files", None)

                files = Caster.discover_files(
                    path_obj, limit_files=limit_files, pattern=pattern
                )
                logger.info(
                    f"For resource name {resource_name} {len(files)} files were found"
                )

                # Create FileDataSource for each file
                for file_path in files:
                    file_source = DataSourceFactory.create_file_data_source(
                        path=file_path
                    )
                    registry.register(file_source, resource_name=resource_name)

            elif resource_type == "table":
                # Handle PostgreSQL table
                pattern = patterns.patterns[resource_name]
                if not isinstance(pattern, TablePattern):
                    logger.warning(
                        f"Pattern for resource '{resource_name}' is not a TablePattern, skipping"
                    )
                    continue

                postgres_config = patterns.get_postgres_config(resource_name)
                if postgres_config is None:
                    logger.warning(
                        f"PostgreSQL table '{resource_name}' has no connection config, skipping"
                    )
                    continue

                # Get table info
                table_info = patterns.get_table_info(resource_name)
                if table_info is None:
                    logger.warning(
                        f"Could not get table info for resource '{resource_name}', skipping"
                    )
                    continue

                table_name, schema_name = table_info
                effective_schema = (
                    schema_name or postgres_config.schema_name or "public"
                )

                # Create SQLDataSource for PostgreSQL table
                try:
                    query = f'SELECT * FROM "{effective_schema}"."{table_name}"'

                    from graflo.data_source.sql import SQLConfig, SQLDataSource
                    from urllib.parse import urlparse

                    parsed = urlparse(postgres_config.uri or "")
                    host = parsed.hostname or "localhost"
                    port = parsed.port or 5432
                    database = (
                        postgres_config.database
                        or parsed.path.lstrip("/")
                        or "postgres"
                    )
                    user = postgres_config.username or parsed.username or "postgres"
                    password = postgres_config.password or parsed.password or ""

                    # Build PostgreSQL connection string
                    if password:
                        connection_string = (
                            f"postgresql://{user}:{password}@{host}:{port}/{database}"
                        )
                    else:
                        connection_string = (
                            f"postgresql://{user}@{host}:{port}/{database}"
                        )

                    # Create SQLDataSource
                    sql_config = SQLConfig(
                        connection_string=connection_string,
                        query=query,
                        pagination=True,
                        page_size=1000,
                    )
                    sql_source = SQLDataSource(config=sql_config)
                    registry.register(sql_source, resource_name=resource_name)

                    logger.info(
                        f"Created SQLDataSource for table '{effective_schema}.{table_name}' "
                        f"mapped to resource '{resource_name}'"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to create data source for PostgreSQL table '{resource_name}': {e}",
                        exc_info=True,
                    )
                    continue

            else:
                logger.warning(
                    f"No pattern configuration found for resource '{resource_name}', skipping"
                )

        # Use the new ingest_data_sources method with output_config
        kwargs["conn_conf"] = output_config
        self.ingest_data_sources(registry, **kwargs)
