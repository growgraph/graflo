"""Data casting and ingestion system for graph databases.

This module provides functionality for casting and ingesting data into graph databases.
It handles batch processing, file discovery, and database operations for both ArangoDB
and Neo4j.

Key Components:
    - Caster: Main class for data casting and ingestion
    - FileConnector: Connector matching for file discovery
    - Connectors: Collection of file connectors for different resources

Ingestion paths (:meth:`ingest`, :meth:`ingest_data_sources`, :meth:`process_resource`,
:meth:`process_data_source`, queue workers) all route batches through
:meth:`process_batch` → :meth:`cast_normal_resource`, which loads the named
``Resource`` from the :class:`~graflo.architecture.contract.declarations.ingestion_model.IngestionModel`
and invokes :meth:`~graflo.architecture.contract.declarations.resource.Resource.__call__` per source document.

Example:
    >>> caster = Caster(schema=schema)
    >>> caster.ingest(path="data/", conn_conf=db_config)
"""

import asyncio
import json
import logging
import sys
import traceback
from pathlib import Path
from typing import Any, cast

import pandas as pd

from suthing import Timer

from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.graph_types import EncodingType, GraphContainer
from graflo.architecture.schema import Schema
from graflo.data_source import (
    AbstractDataSource,
    DataSourceFactory,
    DataSourceRegistry,
)
from graflo.db import DBConfig
from graflo.hq.bulk_session import BulkSessionCoordinator
from graflo.hq.db_writer import DBWriter
from graflo.hq.registry_builder import RegistryBuilder
from graflo.util.chunker import ChunkerType
from graflo.architecture.contract.bindings import Bindings
from graflo.hq.connection_provider import ConnectionProvider, EmptyConnectionProvider
from graflo.hq.doc_error_sink import failure_sinks_from_ingestion_params
from graflo.hq.ingestion_parameters import (
    CastBatchResult,
    DocCastFailure,
    DocErrorBudgetExceeded,
    IngestionParams,
)

logger = logging.getLogger(__name__)

_DOC_CAST_ERROR_TRACEBACK_MAX_CHARS = 16_384


def _filter_graph_container_by_vertices_inplace(
    gc: GraphContainer, *, allowed_vertex_names: set[str] | None
) -> None:
    """Restrict persistence to a subset of vertex types.

    Mutates *gc* in-place, removing:
    - vertex collections whose names are not in *allowed_vertex_names*
    - edge collections whose source/target vertex names are not allowed
    """

    if allowed_vertex_names is None:
        return

    gc.vertices = {
        vcol: items
        for vcol, items in gc.vertices.items()
        if vcol in allowed_vertex_names
    }
    gc.edges = {
        (vfrom, vto, rel): items
        for (vfrom, vto, rel), items in gc.edges.items()
        if vfrom in allowed_vertex_names and vto in allowed_vertex_names
    }


def _format_traceback(exc: BaseException) -> str:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if len(tb) > _DOC_CAST_ERROR_TRACEBACK_MAX_CHARS:
        return tb[:_DOC_CAST_ERROR_TRACEBACK_MAX_CHARS] + "\n...(traceback truncated)"
    return tb


def _build_doc_preview(
    doc: dict[str, Any],
    keys: tuple[str, ...] | None,
    max_bytes: int,
) -> Any:
    if keys is not None:
        preview_obj: Any = {k: doc[k] for k in keys if k in doc}
    else:
        preview_obj = doc
    raw = json.dumps(preview_obj, default=str, sort_keys=True)
    encoded = raw.encode("utf-8")
    if len(encoded) <= max_bytes:
        return json.loads(raw)
    cut = raw.encode("utf-8")[:max_bytes].decode("utf-8", errors="replace")
    return f"{cut}...(doc preview truncated)"


def _doc_failure_from_exception(
    *,
    resource_name: str,
    doc_index: int,
    doc: dict[str, Any],
    exc: BaseException,
    doc_keys: tuple[str, ...] | None,
    doc_preview_max_bytes: int,
) -> DocCastFailure:
    return DocCastFailure(
        resource_name=resource_name,
        doc_index=doc_index,
        exception_type=type(exc).__name__,
        message=str(exc),
        traceback=_format_traceback(exc),
        doc_preview=_build_doc_preview(doc, doc_keys, doc_preview_max_bytes),
    )


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
        ingestion_model: IngestionModel,
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
        self.ingestion_model = ingestion_model
        self._allowed_vertex_names: set[str] | None = None
        self._doc_cast_error_total = 0
        self._doc_cast_error_io_lock = asyncio.Lock()
        self._failure_sinks = failure_sinks_from_ingestion_params(ingestion_params)
        self._bulk_coordinator = BulkSessionCoordinator(schema=self.schema)
        self._ingest_bindings: Bindings | None = None
        self._connection_provider: ConnectionProvider = EmptyConnectionProvider()

    # ------------------------------------------------------------------
    # Casting
    # ------------------------------------------------------------------

    async def _ensure_bulk_session(self, conn_conf: DBConfig) -> str | None:
        """Return active native bulk session id, starting one if needed."""
        return await self._bulk_coordinator.ensure_session(conn_conf)

    async def _finalize_bulk_session(self, conn_conf: DBConfig) -> None:
        await self._bulk_coordinator.finalize(
            conn_conf,
            bindings=self._ingest_bindings,
            connection_provider=self._connection_provider,
        )

    async def _persist_doc_failures(self, failures: list[DocCastFailure]) -> None:
        if not failures:
            return
        params = self.ingestion_params

        async with self._doc_cast_error_io_lock:
            for sink in self._failure_sinks:
                await sink.write_failures(failures)

            self._doc_cast_error_total += len(failures)
            if params.max_doc_errors is not None:
                if self._doc_cast_error_total > params.max_doc_errors:
                    raise DocErrorBudgetExceeded(
                        total_failures=self._doc_cast_error_total,
                        limit=params.max_doc_errors,
                        doc_error_sink_path=params.doc_error_sink_path,
                    )

        if not self._failure_sinks:
            for fail in failures:
                logger.error(
                    "Document cast failure resource=%s doc_index=%s %s: %s",
                    fail.resource_name,
                    fail.doc_index,
                    fail.exception_type,
                    fail.message,
                    extra={"doc_cast_failure": fail.model_dump(mode="json")},
                )

    async def cast_normal_resource(
        self, data, resource_name: str | None = None
    ) -> CastBatchResult:
        """Cast data into a graph container using a resource.

        Args:
            data: Iterable of documents to cast
            resource_name: Optional name of the resource to use

        Returns:
            CastBatchResult with graph and any per-document failures (empty when
            ``on_doc_error`` is ``fail`` and the batch succeeds).
        """
        rr = self.ingestion_model.fetch_resource(resource_name)
        resolved_name = rr.name
        params = self.ingestion_params

        semaphore = asyncio.Semaphore(params.n_cores)

        async def process_doc(doc: dict[str, Any]) -> Any:
            async with semaphore:
                return await asyncio.to_thread(rr, doc)

        if params.on_doc_error == "fail":
            coros = [process_doc(doc) for doc in data]
            docs = await asyncio.gather(*coros)
            graph = GraphContainer.from_docs_list(docs)
            _filter_graph_container_by_vertices_inplace(
                graph, allowed_vertex_names=self._allowed_vertex_names
            )
            return CastBatchResult(graph=graph, failures=[])

        doc_list = list(data)
        raw = await asyncio.gather(
            *[process_doc(doc) for doc in doc_list],
            return_exceptions=True,
        )
        docs: list[Any] = []
        failures: list[DocCastFailure] = []
        for i, item in enumerate(raw):
            doc_raw = doc_list[i]
            doc = (
                doc_raw
                if isinstance(doc_raw, dict)
                else {"_source_repr": repr(doc_raw)}
            )

            if isinstance(item, asyncio.CancelledError):
                raise item
            if isinstance(item, (KeyboardInterrupt, SystemExit)):
                raise item
            if isinstance(item, BaseException):
                failures.append(
                    _doc_failure_from_exception(
                        resource_name=resolved_name,
                        doc_index=i,
                        doc=doc,
                        exc=item,
                        doc_keys=params.doc_error_preview_keys,
                        doc_preview_max_bytes=params.doc_error_preview_max_bytes,
                    )
                )
                continue
            docs.append(item)

        await self._persist_doc_failures(failures)

        graph = GraphContainer.from_docs_list(docs)
        _filter_graph_container_by_vertices_inplace(
            graph, allowed_vertex_names=self._allowed_vertex_names
        )
        return CastBatchResult(graph=graph, failures=failures)

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
        result = await self.cast_normal_resource(batch, resource_name=resource_name)
        if result.failures:
            logger.warning(
                "Resource %r batch had %d document cast failure(s); first: %s: %s",
                result.failures[0].resource_name,
                len(result.failures),
                result.failures[0].exception_type,
                result.failures[0].message,
            )
        gc = result.graph

        if conn_conf is not None:
            writer = self._make_db_writer()
            bulk_sid = await self._ensure_bulk_session(conn_conf)
            await writer.write(
                gc=gc,
                conn_conf=conn_conf,
                resource_name=resource_name,
                bulk_session_id=bulk_sid,
                bindings=self._ingest_bindings,
                connection_provider=self._connection_provider,
            )

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

        # Same semantics as AbstractDataSource.iter_batches(limit=...).
        limit = self.ingestion_params.max_items
        batch_prefetch = self.ingestion_params.batch_prefetch
        queue: asyncio.Queue[list[dict] | object] = asyncio.Queue(
            maxsize=batch_prefetch
        )
        sentinel = object()
        fetch_error: Exception | None = None

        batches_iter = data_source.iter_batches(
            batch_size=self.ingestion_params.batch_size,
            limit=limit,
        )

        def _next_batch_or_sentinel() -> list[dict] | object:
            try:
                return next(batches_iter)
            except StopIteration:
                return sentinel

        async def _produce_batches() -> None:
            nonlocal fetch_error
            try:
                while True:
                    item = await asyncio.to_thread(_next_batch_or_sentinel)
                    await queue.put(item)
                    if item is sentinel:
                        return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fetch_error = exc
                await queue.put(sentinel)

        producer_task = asyncio.create_task(_produce_batches())
        process_error: Exception | None = None
        try:
            while True:
                item = await queue.get()
                if item is sentinel:
                    break
                batch = cast(list[dict], item)
                await self.process_batch(
                    batch,
                    resource_name=actual_resource_name,
                    conn_conf=conn_conf,
                )
        except Exception as exc:
            process_error = exc
            raise
        finally:
            if process_error is not None and not producer_task.done():
                producer_task.cancel()
            try:
                await producer_task
            except asyncio.CancelledError:
                pass

        if fetch_error is not None:
            raise fetch_error

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

    async def ingest_data_sources(
        self,
        data_source_registry: DataSourceRegistry,
        conn_conf: DBConfig,
        ingestion_params: IngestionParams | None = None,
        allowed_resource_names: set[str] | None = None,
        bindings: Bindings | None = None,
        connection_provider: ConnectionProvider | None = None,
    ):
        """Ingest data from data sources in a registry.

        Note: Schema definition should be handled separately via GraphEngine.define_schema()
        before calling this method.

        Args:
            data_source_registry: Registry containing data sources mapped to resources
            conn_conf: Database connection configuration
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
            bindings: Optional manifest bindings (used to resolve S3 staging proxies).
            connection_provider: Runtime credential provider for source connectors and S3.
        """
        if ingestion_params is None:
            ingestion_params = IngestionParams()

        self.ingestion_params = ingestion_params
        self._doc_cast_error_total = 0
        init_only = ingestion_params.init_only

        if init_only:
            logger.info("ingest execution bound to init")
            sys.exit(0)

        self._ingest_bindings = bindings
        self._connection_provider = connection_provider or EmptyConnectionProvider()
        try:
            tasks: list[AbstractDataSource] = []
            for resource_name in self.ingestion_model._resources.keys():
                if (
                    allowed_resource_names is not None
                    and resource_name not in allowed_resource_names
                ):
                    continue
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
        finally:
            await self._finalize_bulk_session(conn_conf)
            self._ingest_bindings = None
            self._connection_provider = EmptyConnectionProvider()

    def ingest(
        self,
        target_db_config: DBConfig,
        bindings: Bindings | None = None,
        ingestion_params: IngestionParams | None = None,
        connection_provider: ConnectionProvider | None = None,
    ):
        """Ingest data into the graph database.

        This is the main ingestion method that takes:
        - Schema: Graph structure (already set in Caster)
        - OutputConfig: Target graph database configuration
        - Bindings: Mapping of resources to physical data sources
        - IngestionParams: Parameters controlling the ingestion process

        Args:
            target_db_config: Target database connection configuration (for writing graph)
            bindings: Bindings instance mapping resources to data sources
                If None, defaults to empty Bindings()
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        bindings = bindings or Bindings()
        ingestion_params = ingestion_params or IngestionParams()

        db_flavor = target_db_config.connection_type
        self.schema.db_profile.db_flavor = db_flavor
        self.schema.finish_init()

        allowed_resource_names = self._resolve_ingestion_scope(ingestion_params)

        self.ingestion_model.finish_init(
            self.schema.core_schema,
            strict_references=ingestion_params.strict_references,
            dynamic_edge_feedback=ingestion_params.dynamic_edges,
            allowed_vertex_names=self._allowed_vertex_names,
            target_db_flavor=db_flavor,
        )

        registry = RegistryBuilder(self.schema, self.ingestion_model).build(
            bindings,
            ingestion_params,
            connection_provider=connection_provider or EmptyConnectionProvider(),
            strict=ingestion_params.strict_registry,
        )

        asyncio.run(
            self.ingest_data_sources(
                data_source_registry=registry,
                conn_conf=target_db_config,
                ingestion_params=ingestion_params,
                allowed_resource_names=allowed_resource_names,
                bindings=bindings,
                connection_provider=connection_provider or EmptyConnectionProvider(),
            )
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_ingestion_scope(
        self, ingestion_params: IngestionParams
    ) -> set[str] | None:
        """Resolve and validate resource/vertex filters for ingestion.

        Resolution order is resources first, then vertices.
        """
        if ingestion_params.resources is not None:
            known_resources = set(self.ingestion_model._resources.keys())
            requested_resources = set(ingestion_params.resources)
            unknown_resources = requested_resources - known_resources
            if unknown_resources:
                raise ValueError(
                    "Unknown resources in ingestion_params.resources: "
                    + ", ".join(sorted(unknown_resources))
                )
            allowed_resource_names: set[str] | None = requested_resources
        else:
            allowed_resource_names = None

        if ingestion_params.vertices is not None:
            known_vertices = {
                v.name for v in self.schema.core_schema.vertex_config.vertices
            }
            requested_vertices = set(ingestion_params.vertices)
            unknown_vertices = requested_vertices - known_vertices
            if unknown_vertices:
                raise ValueError(
                    "Unknown vertices in ingestion_params.vertices: "
                    + ", ".join(sorted(unknown_vertices))
                )
            self._allowed_vertex_names = requested_vertices
        else:
            self._allowed_vertex_names = None

        return allowed_resource_names

    def _make_db_writer(self) -> DBWriter:
        """Create a :class:`DBWriter` from the current ingestion params."""
        max_concurrent = (
            self.ingestion_params.max_concurrent_db_ops
            if self.ingestion_params.max_concurrent_db_ops is not None
            else self.ingestion_params.n_cores
        )
        return DBWriter(
            schema=self.schema,
            ingestion_model=self.ingestion_model,
            dry=self.ingestion_params.dry,
            max_concurrent=max_concurrent,
        )
