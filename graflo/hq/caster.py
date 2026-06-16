"""Data casting and ingestion system for graph databases.

Orchestration (batching, DB writes, queues) lives in :class:`Caster`.
Pure document casting is delegated to :class:`~graflo.hq.document_caster.DocumentCaster`.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, cast

import pandas as pd

from suthing import Timer

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import EncodingType
from graflo.architecture.schema import Schema
from graflo.data_source import (
    AbstractDataSource,
    DataSourceFactory,
    DataSourceRegistry,
)
from graflo.db.connection import DBConfig
from graflo.hq.bulk_session import BulkSessionCoordinator
from graflo.hq.connection_provider import ConnectionProvider, EmptyConnectionProvider
from graflo.hq.db_writer import DBWriter
from graflo.hq.doc_error_sink import failure_sinks_from_ingestion_params
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import (
    CastBatchResult,
    DocCastFailure,
    DocErrorBudgetExceeded,
    IngestionParams,
)
from graflo.hq.registry_builder import RegistryBuilder
from graflo.util.chunker import ChunkerType
from graflo.util.data_normalize import normalize_rows

logger = logging.getLogger(__name__)


class Caster:
    """Ingestion orchestrator: cast documents and write graph batches to the database."""

    def __init__(
        self,
        schema: Schema,
        ingestion_model: IngestionModel,
        ingestion_params: IngestionParams | None = None,
    ):
        if ingestion_params is None:
            ingestion_params = IngestionParams()
        self.ingestion_params = ingestion_params
        self.schema = schema
        self.ingestion_model = ingestion_model
        self._document_caster = DocumentCaster(ingestion_model)
        self._allowed_vertex_names: set[str] | None = None
        self._doc_cast_error_total = 0
        self._doc_cast_error_io_lock = asyncio.Lock()
        self._failure_sinks = failure_sinks_from_ingestion_params(ingestion_params)
        self._bulk_coordinator = BulkSessionCoordinator(schema=self.schema)
        self._ingest_bindings: Bindings | None = None
        self._connection_provider: ConnectionProvider = EmptyConnectionProvider()

    async def _ensure_bulk_session(self, conn_conf: DBConfig) -> str | None:
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
        """Cast data into a graph container using a resource."""
        result = await self._document_caster.cast_batch(
            data,
            resource_name,
            params=self.ingestion_params,
            allowed_vertex_names=self._allowed_vertex_names,
        )
        await self._persist_doc_failures(result.failures)
        return result

    async def process_batch(
        self,
        batch,
        resource_name: str | None,
        conn_conf: None | DBConfig = None,
    ):
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
            )

    async def process_data_source(
        self,
        data_source: AbstractDataSource,
        resource_name: str | None = None,
        conn_conf: None | DBConfig = None,
    ):
        actual_resource_name = resource_name or data_source.resource_name

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

    async def process_with_queue(
        self, tasks: asyncio.Queue, conn_conf: DBConfig | None = None
    ):
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

    @staticmethod
    def normalize_resource(
        data: pd.DataFrame | list[list] | list[dict], columns: list[str] | None = None
    ) -> list[dict]:
        """Normalize resource data into a list of dictionaries."""
        return normalize_rows(data, columns=columns)

    async def ingest_data_sources(
        self,
        data_source_registry: DataSourceRegistry,
        conn_conf: DBConfig,
        ingestion_params: IngestionParams | None = None,
        allowed_resource_names: set[str] | None = None,
        bindings: Bindings | None = None,
        connection_provider: ConnectionProvider | None = None,
    ):
        if ingestion_params is None:
            ingestion_params = IngestionParams()

        self.ingestion_params = ingestion_params
        self._document_caster = DocumentCaster(self.ingestion_model)
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
        bindings = bindings or Bindings()
        ingestion_params = ingestion_params or IngestionParams()

        db_flavor = target_db_config.connection_type
        self.schema.db_profile.db_flavor = db_flavor
        self.schema.finish_init()

        allowed_resource_names = self._resolve_ingestion_scope(
            ingestion_params, bindings=bindings
        )

        self.ingestion_model.finish_init(
            self.schema.core_schema,
            strict_references=ingestion_params.strict_references,
            dynamic_edge_feedback=ingestion_params.dynamic_edges,
            allowed_vertex_names=self._allowed_vertex_names,
            target_db_flavor=db_flavor,
        )
        self._document_caster = DocumentCaster(self.ingestion_model)

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

    def _resolve_ingestion_scope(
        self,
        ingestion_params: IngestionParams,
        *,
        bindings: Bindings | None = None,
    ) -> set[str] | None:
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

        if ingestion_params.connectors is not None:
            if bindings is None:
                raise ValueError(
                    "ingestion_params.connectors requires bindings to resolve connector refs"
                )
            bindings.resolve_connector_refs_to_hashes(ingestion_params.connectors)

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
