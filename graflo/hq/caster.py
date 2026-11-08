"""Data casting and ingestion system for graph databases.

Orchestration (batching, DB writes, queues) lives in :class:`Caster`.
Pure document casting is delegated to :class:`~graflo.hq.document_caster.DocumentCaster`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import Callable, Coroutine, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self, cast

import pandas as pd
from suthing import Timer

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.sync import owned_edges, owned_types
from graflo.architecture.graph_types import EncodingType, GraphContainer
from graflo.architecture.schema import Schema
from graflo.connections.bulk import BulkConfig
from graflo.connections.onto import DBConfig
from graflo.connections.provider import ConnectionProvider, EmptyConnectionProvider
from graflo.data_source.base import AbstractDataSource
from graflo.data_source.chunker import ChunkerType
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import FilterExpression
from graflo.hq.concurrency_gate import SerialReason, effective_in_flight
from graflo.hq.db_writer import DBWriter, check_stamp, require_read_guard
from graflo.hq.doc_error_sink import failure_sinks_from_ingestion_params
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import (
    CastBatchResult,
    DocCastFailure,
    DocErrorBudgetExceeded,
    IngestionParams,
)
from graflo.hq.registry_builder import RegistryBuilder
from graflo.hq.write_prepare import WriteContext
from graflo.util.data_normalize import normalize_rows

if TYPE_CHECKING:
    from graflo.hq.bulk.stage import Stager

logger = logging.getLogger(__name__)


def _log_nothing_cast(resource_name: str | None) -> None:
    logger.debug(
        "A batch from resource %r cast to no document; not written", resource_name
    )


def _hook_key(
    stamp: Mapping[str, Any] | None, guard: FilterExpression | None
) -> tuple[str, str]:
    """A hashable key equal for equal stamp and guard values: their canonical JSON."""
    return (
        json.dumps(
            None if stamp is None else dict(stamp), sort_keys=True, default=repr
        ),
        json.dumps(
            None if guard is None else guard.model_dump(), sort_keys=True, default=repr
        ),
    )


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
        # Error-budget accounting is shared by all in-flight batches and sources;
        # it stays correct because every writer runs on the one event loop and
        # serializes on this asyncio.Lock.
        self._doc_cast_error_total = 0
        self._doc_cast_error_io_lock = asyncio.Lock()
        self._failure_sinks = failure_sinks_from_ingestion_params(ingestion_params)
        # One writer per run: the db-aware schema projection is resolved once and
        # its semaphore bounds DB operations across all in-flight batches.
        self._db_writer: DBWriter | None = None
        # Set for the staging phase of a bulk run: batches are staged, not written.
        self._stager: Stager | None = None
        # (resource ordinal, source position) of each data source of the run.
        self._source_seq: dict[int, tuple[int, int]] = {}

    def close(self) -> None:
        """Release the cast worker pools and the writer's database connections.

        Ingestion runs call it when they end; a caller driving
        :meth:`process_batch` or :meth:`process_resource` itself calls it (or
        uses the caster as a context manager) when done. The caster stays usable.
        """
        self._document_caster.close()
        if self._db_writer is not None:
            self._db_writer.close()
            self._db_writer = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    async def _persist_doc_failures(self, failures: list[DocCastFailure]) -> None:
        if not failures:
            return
        params = self.ingestion_params

        async with self._doc_cast_error_io_lock:
            for sink in self._failure_sinks:
                await sink.write_failures(failures)

            self._doc_cast_error_total += len(failures)
            if (
                params.max_doc_errors is not None
                and self._doc_cast_error_total > params.max_doc_errors
            ):
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
        *,
        batch_seq: tuple[int, int, int] | None = None,
    ):
        """Cast *batch* and write it to *conn_conf* (cast only when ``None``).

        In the staging phase of a bulk run it is staged instead, as batch
        *batch_seq* ``(resource, source, batch)``.
        """
        params = self.ingestion_params
        if params.stamp_for is not None or params.guard_for is not None:
            await self._process_batch_per_record(
                batch, resource_name, conn_conf, batch_seq=batch_seq
            )
            return
        result = await self.cast_normal_resource(batch, resource_name=resource_name)
        self._warn_failures(result.failures)
        gc = result.graph
        if conn_conf is None:
            return
        if gc.is_empty():
            _log_nothing_cast(resource_name)
            return
        await self._emit(gc, conn_conf, resource_name, batch_seq=batch_seq)

    async def _emit(
        self,
        gc: GraphContainer,
        conn_conf: DBConfig,
        resource_name: str | None,
        *,
        batch_seq: tuple[int, int, int] | None,
        part: int = 0,
        stamp: Mapping[str, Any] | None = None,
        guard: FilterExpression | None = None,
    ) -> None:
        """Write *gc*, or stage it as part *part* of batch *batch_seq* in a bulk run."""
        if self._stager is not None:
            if batch_seq is None:
                raise RuntimeError(
                    "a bulk run stages the batches of its data sources only"
                )
            await asyncio.to_thread(
                self._stager.stage, gc, (*batch_seq, part), stamp=stamp
            )
            return
        writer = self._get_db_writer(conn_conf)
        await writer.write(
            gc=gc,
            conn_conf=conn_conf,
            resource_name=resource_name,
            stamp=stamp,
            guard=guard,
        )

    async def _process_batch_per_record(
        self,
        batch: list[Any],
        resource_name: str | None,
        conn_conf: DBConfig | None,
        *,
        batch_seq: tuple[int, int, int] | None = None,
    ) -> None:
        """Cast *batch* and write each record's output under its own stamp and guard.

        ``stamp_for`` and ``guard_for`` are called on every record before it is
        cast. Records with equal stamps and guards are written together, one
        :meth:`DBWriter.write` per distinct pair, in the order the pair first
        appears; nothing one record casts to is written with another's stamp.
        Every stamp, and the backend's support for any guard, is checked before
        the first write, so a batch that would be refused writes nothing.
        """
        params = self.ingestion_params
        records = list(batch)
        name = self.ingestion_model.fetch_resource(resource_name).name
        hooks: dict[
            tuple[str, str], tuple[Mapping[str, Any] | None, FilterExpression | None]
        ] = {}
        keys: list[tuple[str, str]] = []
        for record in records:
            stamp = params.stamp_for(name, record) if params.stamp_for else None
            guard = params.guard_for(name, record) if params.guard_for else None
            key = _hook_key(stamp, guard)
            hooks.setdefault(key, (stamp, guard))
            keys.append(key)

        parts, failures = await self._document_caster.cast_batch_by_key(
            records,
            keys,
            resource_name,
            params=params,
            allowed_vertex_names=self._allowed_vertex_names,
        )
        await self._persist_doc_failures(failures)
        self._warn_failures(failures)
        if conn_conf is None:
            return
        for key, gc in parts:
            check_stamp(gc, self.schema, hooks[cast(tuple[str, str], key)][0])
        if any(guard is not None for _, guard in hooks.values()):
            require_read_guard(conn_conf)
        parts = [(key, gc) for key, gc in parts if not gc.is_empty()]
        if not parts:
            _log_nothing_cast(resource_name)
            return
        for part, (key, gc) in enumerate(parts):
            stamp, guard = hooks[cast(tuple[str, str], key)]
            await self._emit(
                gc,
                conn_conf,
                resource_name,
                batch_seq=batch_seq,
                part=part,
                stamp=stamp,
                guard=guard,
            )

    @staticmethod
    def _warn_failures(failures: list[DocCastFailure]) -> None:
        if failures:
            logger.warning(
                "Resource %r batch had %d document cast failure(s); first: %s: %s",
                failures[0].resource_name,
                len(failures),
                failures[0].exception_type,
                failures[0].message,
            )

    async def process_data_source(
        self,
        data_source: AbstractDataSource,
        resource_name: str | None = None,
        conn_conf: None | DBConfig = None,
    ):
        actual_resource_name = resource_name or data_source.resource_name
        try:
            await self._drain_data_source(
                data_source, resource_name=actual_resource_name, conn_conf=conn_conf
            )
        except BaseException:
            await self._close_source(data_source, quiet=True)
            raise
        await self._close_source(data_source)

    async def _drain_data_source(
        self,
        data_source: AbstractDataSource,
        *,
        resource_name: str | None,
        conn_conf: DBConfig | None,
    ) -> None:
        """Cast and write every batch of *data_source*, acknowledging each once written."""
        in_flight = self._effective_in_flight(resource_name)

        limit = self.ingestion_params.max_items
        batch_prefetch = self.ingestion_params.batch_prefetch
        queue: asyncio.Queue[tuple[int, list[dict]] | object] = asyncio.Queue(
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
            index = 0
            try:
                while True:
                    item = await asyncio.to_thread(_next_batch_or_sentinel)
                    if item is sentinel:
                        await queue.put(sentinel)
                        return
                    # Numbered in the order the source yielded them, which is
                    # how the source is told which one was written.
                    await queue.put((index, item))
                    index += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fetch_error = exc
                await queue.put(sentinel)

        source_seq = self._source_seq.get(id(data_source))

        async def _write_batch(index: int, batch: list[dict]) -> None:
            await self.process_batch(
                batch,
                resource_name=resource_name,
                conn_conf=conn_conf,
                batch_seq=None if source_seq is None else (*source_seq, index),
            )
            self._batch_written(data_source, index, conn_conf)

        producer_task = asyncio.create_task(_produce_batches())
        process_error: Exception | None = None
        try:
            if in_flight <= 1:
                while True:
                    item = await queue.get()
                    if item is sentinel:
                        break
                    await _write_batch(*cast(tuple[int, list[dict]], item))
            else:
                await self._process_batches_pipelined(
                    queue,
                    sentinel,
                    in_flight=in_flight,
                    handle=_write_batch,
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

    def _batch_written(
        self,
        data_source: AbstractDataSource,
        index: int,
        conn_conf: DBConfig | None,
    ) -> None:
        """Tell *data_source* that batch *index* is in the target.

        Nothing is said when nothing was written (no target, a dry run, a
        batch staged for a bulk load).
        """
        if conn_conf is None or self.ingestion_params.dry or self._stager is not None:
            return
        data_source.acknowledge(index)

    @staticmethod
    async def _close_source(
        data_source: AbstractDataSource, *, quiet: bool = False
    ) -> None:
        """Close *data_source*.

        *quiet* logs a failure to close instead of raising, for the path where
        another error is already on its way out.
        """
        try:
            await asyncio.to_thread(data_source.close)
        except Exception:
            if not quiet:
                raise
            logger.warning("Closing a data source failed", exc_info=True)

    def _gate_resource(
        self, resource_name: str | None
    ) -> tuple[int, SerialReason | None]:
        """Gate result for one resource: allowed in-flight batches and why not."""
        try:
            runtime = self.ingestion_model.fetch_resource(resource_name)
        except (ValueError, RuntimeError):
            runtime = None
        return effective_in_flight(runtime, self.ingestion_params)

    def _effective_in_flight(self, resource_name: str | None) -> int:
        """Resolve how many batches of this source may be in flight at once."""
        in_flight, serial_reason = self._gate_resource(resource_name)
        if (
            serial_reason is not None
            and serial_reason is not SerialReason.USER_OVERRIDE
        ):
            logger.info(
                "Resource %r: batches processed serially (%s)",
                resource_name,
                serial_reason.value,
            )
        return in_flight

    async def _process_batches_pipelined(
        self,
        queue: asyncio.Queue,
        sentinel: object,
        *,
        in_flight: int,
        handle: Callable[[int, list[dict]], Coroutine[Any, Any, None]],
    ) -> None:
        """Run *handle* on each numbered batch, up to *in_flight* at a time.

        Casting of batch N+1 overlaps the DB write of batch N. Error contract
        matches the serial loop: the first exception (by completion order)
        propagates bare after in-flight siblings are cancelled — deliberately
        not a ``TaskGroup``, whose ``ExceptionGroup`` would break callers that
        catch e.g. :class:`DocErrorBudgetExceeded` directly.
        """
        sem = asyncio.Semaphore(in_flight)
        pending: set[asyncio.Task] = set()
        first_error: BaseException | None = None

        def _on_done(task: asyncio.Task) -> None:
            nonlocal first_error
            pending.discard(task)
            if not task.cancelled():
                exc = task.exception()
                if exc is not None and first_error is None:
                    first_error = exc
            sem.release()

        try:
            while True:
                await sem.acquire()
                if first_error is not None:
                    sem.release()
                    break
                item = await queue.get()
                if item is sentinel:
                    sem.release()
                    break
                index, batch = cast(tuple[int, list[dict]], item)
                task = asyncio.create_task(handle(index, batch))
                pending.add(task)
                task.add_done_callback(_on_done)

            if first_error is None and pending:
                done, _ = await asyncio.wait(set(pending))
                for task in done:
                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc is not None and first_error is None:
                        first_error = exc
        finally:
            remaining = set(pending)
            for task in remaining:
                task.cancel()
            if remaining:
                await asyncio.gather(*remaining, return_exceptions=True)

        if first_error is not None:
            raise first_error

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
            config: dict[str, Any] = dict(resource_instance)
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

    async def _process_source_group(
        self,
        group: list[AbstractDataSource],
        *,
        conn_conf: DBConfig | None,
        resource_name: str,
    ) -> None:
        """Process one resource's data sources, fanning out when there are many."""
        source_workers = (
            self.ingestion_params.max_concurrent_sources
            if self.ingestion_params.max_concurrent_sources is not None
            # Sources of one resource are independent shards (one file each);
            # interleaving them is safe. The fallback no longer hides behind
            # n_cores, which governs CPU-bound cast workers.
            else min(4, len(group))
        )
        # A resource the gate serializes must not fan its sources out either:
        # concurrent sources share the same ResourceRuntime (dynamic_edges would
        # race its mid-cast registrations) and reintroduce exactly the cross-batch
        # ordering hazards the gate exists to prevent. USER_OVERRIDE is excluded —
        # max_in_flight_batches=1 targets batches; sources have their own knob.
        _, serial_reason = self._gate_resource(resource_name)
        if (
            serial_reason is not None
            and serial_reason is not SerialReason.USER_OVERRIDE
            and source_workers > 1
        ):
            logger.info(
                "Resource %r: sources processed serially (%s)",
                resource_name,
                serial_reason.value,
            )
            source_workers = 1
        if source_workers <= 1 or len(group) <= 1:
            for data_source in group:
                await self.process_data_source(
                    data_source=data_source, conn_conf=conn_conf
                )
            return

        queue_tasks: asyncio.Queue = asyncio.Queue()
        for item in group:
            await queue_tasks.put(item)
        for _ in range(source_workers):
            await queue_tasks.put(None)

        worker_tasks = [
            asyncio.create_task(
                self.process_with_queue(queue_tasks, conn_conf=conn_conf)
            )
            for _ in range(source_workers)
        ]
        try:
            await asyncio.gather(*worker_tasks)
        except BaseException:
            for wt in worker_tasks:
                wt.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            raise

    async def process_with_queue(
        self, tasks: asyncio.Queue, conn_conf: DBConfig | None = None
    ):
        """Drain data-source tasks from *tasks* until a sentinel (``None``).

        Exceptions propagate to the caller (``ingest_data_sources`` cancels the
        sibling workers); they used to be logged and swallowed here, which made
        a failed source indistinguishable from a finished one.
        """
        SENTINEL = None

        while True:
            task = await tasks.get()
            try:
                if task is SENTINEL:
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
            finally:
                tasks.task_done()

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
    ):
        if ingestion_params is None:
            ingestion_params = IngestionParams()

        self.ingestion_params = ingestion_params
        self._document_caster = DocumentCaster(self.ingestion_model)
        self._doc_cast_error_total = 0
        self._db_writer = None
        init_only = ingestion_params.init_only

        if init_only:
            logger.info("ingest execution bound to init")
            sys.exit(0)

        try:
            resource_groups: list[tuple[str, list[AbstractDataSource]]] = []
            self._source_seq = {}
            for ordinal, resource_name in enumerate(self.ingestion_model._resources):
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
                    resource_groups.append((resource_name, data_sources))
                    for position, data_source in enumerate(data_sources):
                        self._source_seq[id(data_source)] = (ordinal, position)

            with Timer() as klepsidra:
                # Resource declaration order is semantic: a later resource may
                # depend on database state written by an earlier one (edges over
                # another resource's vertices, secondary-identity endpoint
                # resolution, extra weights). Sources fan out *within* a
                # resource — they are independent shards of the same stream —
                # with a barrier between resources.
                for resource_name, group in resource_groups:
                    await self._process_source_group(
                        group, conn_conf=conn_conf, resource_name=resource_name
                    )
            logger.info(f"Processing took {klepsidra.elapsed:.1f} sec")
        finally:
            if self._db_writer is not None and not self._db_writer.stats.is_empty():
                logger.info("Write resolution:\n%s", self._db_writer.stats.summary())
            # The cast pool and the writer's connections outlive individual
            # batches on purpose; the run is where they stop.
            self.close()

    def ingest(
        self,
        target_db_config: DBConfig,
        bindings: Bindings | None = None,
        ingestion_params: IngestionParams | None = None,
        connection_provider: ConnectionProvider | None = None,
        data_source_registry: DataSourceRegistry | None = None,
        *,
        recreate_schema: bool = False,
    ):
        """Cast and write every resource's sources into *target_db_config*.

        The sources are built from *bindings*, unless *data_source_registry*
        supplies them. In bulk mode the run defines the schema itself, dropping
        an existing one first only when *recreate_schema*.
        """
        bindings = bindings or Bindings()
        ingestion_params = ingestion_params or IngestionParams()

        allowed_resource_names = self.validate_run(
            target_db_config, bindings, ingestion_params
        )

        if data_source_registry is not None:
            registry = data_source_registry
        else:
            registry = RegistryBuilder(self.schema, self.ingestion_model).build(
                bindings,
                ingestion_params,
                connection_provider=connection_provider or EmptyConnectionProvider(),
                strict=ingestion_params.strict_registry,
            )

        if ingestion_params.mode == "bulk":
            self._ingest_bulk(
                registry,
                target_db_config,
                ingestion_params,
                allowed_resource_names,
                recreate_schema=recreate_schema,
            )
            return
        if ingestion_params.mode == "sync":
            if recreate_schema:
                raise ValueError("recreate_schema is for a bulk run")
            self._ingest_sync(
                registry, target_db_config, ingestion_params, allowed_resource_names
            )
            return
        if recreate_schema:
            raise ValueError("recreate_schema is for a bulk run; call define_schema")
        asyncio.run(
            self.ingest_data_sources(
                data_source_registry=registry,
                conn_conf=target_db_config,
                ingestion_params=ingestion_params,
                allowed_resource_names=allowed_resource_names,
            )
        )

    def _ingest_bulk(
        self,
        registry: DataSourceRegistry,
        conn_conf: DBConfig,
        ingestion_params: IngestionParams,
        allowed_resource_names: set[str] | None,
        *,
        recreate_schema: bool,
    ) -> None:
        """Stage every batch, fold, then write once; resume a stopped run.

        Owned types carrying a field set by sync get their owners' hashes, so
        the first sync after the load skips every unchanged record.
        """
        from graflo.hq.bulk.run import (
            dense_ids_for,
            edge_buckets_for,
            fold_staged,
            open_staging,
            write_folded,
        )

        staging_dir = cast(Path, ingestion_params.staging_dir)
        cfg = conn_conf.bulk or BulkConfig()
        ledger = open_staging(staging_dir, "bulk")
        self._stage(
            registry, conn_conf, ingestion_params, allowed_resource_names, ledger
        )
        counts = fold_staged(
            staging_dir,
            ledger,
            cfg,
            dense_ids=dense_ids_for(conn_conf),
            edge_buckets=edge_buckets_for(conn_conf, cfg),
        )
        self._group(staging_dir, counts, ledger, allowed_resource_names)
        write_folded(
            self.schema, conn_conf, staging_dir, ledger, cfg, recreate=recreate_schema
        )

    def _ingest_sync(
        self,
        registry: DataSourceRegistry,
        conn_conf: DBConfig,
        ingestion_params: IngestionParams,
        allowed_resource_names: set[str] | None,
    ) -> None:
        """Stage, fold and group as a bulk run does, then rewrite changed owners."""
        from graflo.hq.bulk.run import fold_staged, open_staging
        from graflo.hq.sync.run import check_key_only, write_sync

        staging_dir = cast(Path, ingestion_params.staging_dir)
        cfg = conn_conf.bulk or BulkConfig()
        ledger = open_staging(staging_dir, "sync")
        self._stage(
            registry, conn_conf, ingestion_params, allowed_resource_names, ledger
        )
        counts = fold_staged(staging_dir, ledger, cfg, dense_ids=False)
        owner_runs = self._group(staging_dir, counts, ledger, allowed_resource_names)
        check_key_only(
            self.schema, staging_dir, counts, {run.owner for run in owner_runs}
        )
        write_sync(
            self.schema,
            conn_conf,
            staging_dir,
            ledger,
            cfg,
            owner_runs,
            owned_edges(self._in_scope(allowed_resource_names)),
        )

    def _in_scope(self, allowed_resource_names: set[str] | None) -> list[Any]:
        return [
            r
            for r in self.ingestion_model.resources
            if allowed_resource_names is None or r.name in allowed_resource_names
        ]

    def _stage(
        self,
        registry: DataSourceRegistry,
        conn_conf: DBConfig,
        ingestion_params: IngestionParams,
        allowed_resource_names: set[str] | None,
        ledger: Any,
    ) -> None:
        """Cast every batch into the staging directory, unless it is staged."""
        from graflo.hq.bulk.ledger import Phase
        from graflo.hq.bulk.stage import Stager

        if ledger.has(Phase.STAGED):
            return
        ctx = WriteContext.for_flavor(self.schema, conn_conf.connection_type)
        self._stager = Stager(
            self.schema,
            ctx,
            cast(Path, ingestion_params.staging_dir),
            owned_edges(self._in_scope(allowed_resource_names)),
        )
        try:
            asyncio.run(
                self.ingest_data_sources(
                    data_source_registry=registry,
                    conn_conf=conn_conf,
                    ingestion_params=ingestion_params,
                    allowed_resource_names=allowed_resource_names,
                )
            )
        finally:
            self._stager = None
        ledger.mark(Phase.STAGED)

    def _group(
        self,
        staging_dir: Path,
        counts: Any,
        ledger: Any,
        allowed_resource_names: set[str] | None,
    ) -> list[Any]:
        """Group the owned types that carry a hash field; set their owners' hashes."""
        from graflo.hq.bulk.ledger import Phase
        from graflo.hq.sync.group import group_owners, grouped_owners
        from graflo.hq.sync.refusals import hash_fields

        if ledger.has(Phase.GROUPED):
            return grouped_owners(staging_dir)
        fields = hash_fields(
            self.schema, owned_types(self._in_scope(allowed_resource_names))
        )
        runs = group_owners(staging_dir, counts, fields)
        ledger.mark(Phase.GROUPED)
        return runs

    def validate_run(
        self,
        target_db_config: DBConfig,
        bindings: Bindings,
        ingestion_params: IngestionParams,
    ) -> set[str] | None:
        """Check the run's scope and the pipelines against the schema, and return
        the resources in scope (``None``: all).

        Raises on a reference ``ingestion_params.strict_references`` refuses.
        Touches no database, so callers run it before anything that does.
        """
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
        if ingestion_params.mode == "bulk":
            self._validate_bulk(target_db_config, allowed_resource_names)
        if ingestion_params.mode == "sync":
            self._validate_sync(target_db_config, allowed_resource_names)
        return allowed_resource_names

    def _validate_sync(
        self, target_db_config: DBConfig, allowed_resource_names: set[str] | None
    ) -> None:
        """Refuse a sync run that could not complete, before anything is read.

        Raises:
            RuntimeError: The ``bulk`` extra is not installed.
            ValueError: The target has no sync writer, a resource in scope reads
                the target while ingesting, or writes what no sync block owns.
        """
        from graflo.hq.bulk import _deps
        from graflo.hq.bulk.refusals import bulk_refusals
        from graflo.hq.sync.refusals import sync_refusals

        _deps.require()
        ConnectionManager.require(target_db_config, ConnectionCapability.SYNC)
        scope = (
            None if allowed_resource_names is None else sorted(allowed_resource_names)
        )
        reasons = bulk_refusals(self.schema, self.ingestion_model, scope)
        if reasons:
            raise ValueError(
                "sync mode cannot run what reads the target while ingesting: "
                + "; ".join(reasons)
            )
        reasons = sync_refusals(self.schema, self.ingestion_model, scope)
        if reasons:
            raise ValueError("sync mode: " + "; ".join(reasons))

    def _validate_bulk(
        self, target_db_config: DBConfig, allowed_resource_names: set[str] | None
    ) -> None:
        """Refuse a bulk run that could not complete, before anything is read.

        Raises:
            RuntimeError: The ``bulk`` extra is not installed.
            ValueError: The target has no bulk writer, a resource in scope
                reads the target while ingesting, or the schema has a union
                field the target's upsert refuses.
        """
        from graflo.hq.bulk import _deps
        from graflo.hq.bulk.refusals import bulk_refusals, union_fields

        _deps.require()
        ConnectionManager.require(target_db_config, ConnectionCapability.BULK_LOAD)
        unions = union_fields(self.schema)
        if unions:
            try:
                ConnectionManager.require(
                    target_db_config, ConnectionCapability.UNION_MERGE
                )
            except ValueError as error:
                raise ValueError(
                    f"bulk mode cannot load union fields {unions}: no later upsert "
                    f"could extend them. {error}"
                ) from error
        reasons = bulk_refusals(
            self.schema,
            self.ingestion_model,
            None if allowed_resource_names is None else sorted(allowed_resource_names),
        )
        if reasons:
            raise ValueError(
                "bulk mode cannot load what reads the target while ingesting: "
                + "; ".join(reasons)
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
        max_concurrent = self.ingestion_params.max_concurrent_db_ops
        return DBWriter(
            schema=self.schema,
            ingestion_model=self.ingestion_model,
            dry=self.ingestion_params.dry,
            max_concurrent=max_concurrent,
        )

    def _get_db_writer(self, conn_conf: DBConfig) -> DBWriter:
        """Run-scoped writer, created on first use (synchronously on the loop).

        Reusing one writer keeps ``schema.resolve_db_aware`` to a single call
        per run and makes ``max_concurrent_db_ops`` a run-wide bound.
        """
        if self._db_writer is None:
            writer = self._make_db_writer()
            # Pre-warm the db-aware projection so concurrent write() calls never
            # race the lazy cache fill.
            writer.context(conn_conf)
            self._db_writer = writer
        return self._db_writer
