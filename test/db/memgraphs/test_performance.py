"""Performance and load tests for Memgraph connector.

This module provides comprehensive performance benchmarks and load tests
that measure connector behavior under stress. Tests are designed to identify
performance limits, bottlenecks, and degradation patterns.

Test Categories
---------------
Throughput
    Measures operations per second for various operation types including
    single inserts, batch inserts, reads, and queries.

Scalability
    Tests performance degradation as data volume increases, measuring
    both write and query performance at different dataset sizes.

Concurrency
    Tests parallel access patterns including concurrent writes,
    concurrent reads, and mixed read/write workloads.

BatchSizing
    Determines optimal batch sizes for bulk operations by comparing
    throughput across different batch configurations.

SustainedLoad
    Tests system stability under prolonged load, measuring latency
    drift and throughput consistency over time.

Limits
    Tests system limits including maximum property counts, batch sizes,
    and connection pool capacity.

GraphOperations
    Tests performance of graph-specific operations including edge
    creation and traversal queries.

Usage
-----
Run all performance tests::

    pytest test/db/memgraphs/test_performance.py -v -s

Run quick tests only (exclude slow markers)::

    pytest test/db/memgraphs/test_performance.py -v -k "not slow"

Run sustained load tests::

    pytest test/db/memgraphs/test_performance.py -v -k "sustained"

Notes
-----
- Performance thresholds are hardware-dependent and should be adjusted
- Tests marked with @pytest.mark.slow are excluded from quick CI runs
- Use -s flag to see performance metrics output
- Results vary based on Memgraph configuration and available resources

See Also
--------
- graflo.db.memgraph.conn : Memgraph connector implementation
- test.db.memgraphs.test_functional : Functional correctness tests
- test.db.memgraphs.test_edge_cases : Technical edge case tests
"""

import concurrent.futures
import gc
import statistics
import threading
import time
from dataclasses import dataclass
from typing import Callable

import pytest

from graflo.db import ConnectionManager
from graflo.onto import AggregationType


# =============================================================================
# HELPERS AND METRICS
# =============================================================================


@dataclass
class BenchmarkResult:
    """Benchmark result data container."""

    operation: str
    total_ops: int
    duration_sec: float
    ops_per_sec: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float

    def __str__(self):
        return (
            f"\n{'=' * 60}\n"
            f"  {self.operation}\n"
            f"{'=' * 60}\n"
            f"  Total ops:     {self.total_ops:,}\n"
            f"  Duration:      {self.duration_sec:.2f}s\n"
            f"  Throughput:    {self.ops_per_sec:,.0f} ops/sec\n"
            f"  Latency (ms):\n"
            f"    avg: {self.avg_latency_ms:.2f}\n"
            f"    p50: {self.p50_latency_ms:.2f}\n"
            f"    p95: {self.p95_latency_ms:.2f}\n"
            f"    p99: {self.p99_latency_ms:.2f}\n"
            f"    min: {self.min_latency_ms:.2f}\n"
            f"    max: {self.max_latency_ms:.2f}\n"
        )


def percentile(data: list, p: float) -> float:
    """Calculate the p-th percentile of a list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def benchmark(operation_name: str, ops: int, func: Callable) -> BenchmarkResult:
    """Execute a benchmark and collect metrics."""
    latencies = []

    start_total = time.perf_counter()
    for _ in range(ops):
        start = time.perf_counter()
        func()
        latencies.append((time.perf_counter() - start) * 1000)  # in ms
    duration = time.perf_counter() - start_total

    return BenchmarkResult(
        operation=operation_name,
        total_ops=ops,
        duration_sec=duration,
        ops_per_sec=ops / duration if duration > 0 else 0,
        avg_latency_ms=statistics.mean(latencies) if latencies else 0,
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
        min_latency_ms=min(latencies) if latencies else 0,
        max_latency_ms=max(latencies) if latencies else 0,
    )


# =============================================================================
# THROUGHPUT TESTS
# =============================================================================


class TestThroughput:
    """Throughput tests - operations per second measurements."""

    def test_single_insert_throughput(self, conn_conf, test_graph_name, clean_db):
        """Measure single insert throughput."""
        _ = clean_db
        ops = 500

        with ConnectionManager(connection_config=conn_conf) as db:
            counter = [0]

            def insert_one():
                """Insert a single document with incrementing ID."""
                counter[0] += 1
                db.upsert_docs_batch(
                    [{"id": str(counter[0]), "data": "x" * 100}],
                    "Throughput",
                    match_keys=["id"],
                )

            result = benchmark("Single Insert", ops, insert_one)
            print(result)

            # Verify results
            count = db.aggregate("Throughput", AggregationType.COUNT)
            assert count == ops

            # Minimum expected threshold (adjust based on hardware)
            assert result.ops_per_sec > 50, (
                f"Throughput too low: {result.ops_per_sec:.0f} ops/s"
            )

    def test_batch_insert_throughput(self, conn_conf, test_graph_name, clean_db):
        """Measure batch insert throughput."""
        _ = clean_db
        batch_size = 100
        num_batches = 50

        with ConnectionManager(connection_config=conn_conf) as db:
            counter = [0]

            def insert_batch():
                """Insert a batch of documents with sequential IDs."""
                start_id = counter[0] * batch_size
                counter[0] += 1
                docs = [
                    {"id": str(start_id + i), "data": "x" * 100}
                    for i in range(batch_size)
                ]
                db.upsert_docs_batch(docs, "BatchThroughput", match_keys=["id"])

            result = benchmark(
                f"Batch Insert (size={batch_size})", num_batches, insert_batch
            )

            # Calculate throughput in documents/sec
            docs_per_sec = (num_batches * batch_size) / result.duration_sec
            print(result)
            print(f"  Documents/sec: {docs_per_sec:,.0f}")

            count = db.aggregate("BatchThroughput", AggregationType.COUNT)
            assert count == num_batches * batch_size

    def test_read_throughput(self, conn_conf, test_graph_name, clean_db):
        """Measure read throughput."""
        _ = clean_db
        num_docs = 1000
        read_ops = 500

        with ConnectionManager(connection_config=conn_conf) as db:
            # Setup: insert test data
            docs = [{"id": str(i), "value": i} for i in range(num_docs)]
            db.upsert_docs_batch(docs, "ReadTest", match_keys=["id"])

            def read_all():
                """Read up to 100 documents from test collection."""
                db.fetch_docs("ReadTest", limit=100)

            result = benchmark("Read (limit=100)", read_ops, read_all)
            print(result)

            assert result.ops_per_sec > 100, (
                f"Read throughput too low: {result.ops_per_sec:.0f}"
            )

    def test_query_throughput(self, conn_conf, test_graph_name, clean_db):
        """Measure Cypher query throughput."""
        _ = clean_db
        num_docs = 1000
        query_ops = 300

        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": str(i), "category": f"cat_{i % 10}", "value": i}
                for i in range(num_docs)
            ]
            db.upsert_docs_batch(docs, "QueryTest", match_keys=["id"])

            counter = [0]

            def run_query():
                """Execute a filtered count query on rotating categories."""
                cat = f"cat_{counter[0] % 10}"
                counter[0] += 1
                db.execute(
                    f"MATCH (n:QueryTest) WHERE n.category = '{cat}' RETURN count(n)"
                )

            result = benchmark("Filtered Query", query_ops, run_query)
            print(result)


# =============================================================================
# SCALABILITY TESTS
# =============================================================================


class TestScalability:
    """Scalability tests - behavior with increasing data volumes."""

    def test_insert_scaling(self, conn_conf, test_graph_name, clean_db):
        """Measure performance degradation as data volume increases."""
        _ = clean_db
        batch_size = 100
        checkpoints = [100, 500, 1000, 2000, 5000]

        results = []

        with ConnectionManager(connection_config=conn_conf) as db:
            total_inserted = 0

            for target in checkpoints:
                batches_needed = (target - total_inserted) // batch_size

                start = time.perf_counter()
                for b in range(batches_needed):
                    start_id = total_inserted + b * batch_size
                    docs = [
                        {"id": str(start_id + i), "data": f"value_{start_id + i}"}
                        for i in range(batch_size)
                    ]
                    db.upsert_docs_batch(docs, "ScaleTest", match_keys=["id"])
                duration = time.perf_counter() - start

                total_inserted = target
                docs_per_sec = (
                    (batches_needed * batch_size) / duration if duration > 0 else 0
                )
                results.append((target, docs_per_sec))

            print("\n" + "=" * 60)
            print("  Insert Scaling (docs/sec vs total docs)")
            print("=" * 60)
            for total, rate in results:
                bar = "#" * int(rate / 100)
                print(f"  {total:>6,} docs: {rate:>8,.0f} docs/sec {bar}")

            # Document degradation (informational)
            if results[0][1] > 0:
                degradation = results[-1][1] / results[0][1]
                print(f"\n  Degradation: {(1 - degradation) * 100:.1f}%")

    def test_query_scaling_with_data_volume(self, conn_conf, test_graph_name, clean_db):
        """Measure query latency impact as data volume increases."""
        _ = clean_db
        checkpoints = [100, 500, 1000, 5000]
        queries_per_checkpoint = 50

        results = []

        with ConnectionManager(connection_config=conn_conf) as db:
            total_docs = 0

            for target in checkpoints:
                # Add documents until checkpoint
                docs_to_add = target - total_docs
                if docs_to_add > 0:
                    docs = [
                        {"id": str(total_docs + i), "category": f"cat_{i % 10}"}
                        for i in range(docs_to_add)
                    ]
                    db.upsert_docs_batch(docs, "QueryScale", match_keys=["id"])
                    total_docs = target

                # Measure query latencies
                latencies = []
                for i in range(queries_per_checkpoint):
                    cat = f"cat_{i % 10}"
                    start = time.perf_counter()
                    db.fetch_docs("QueryScale", filters=["==", cat, "category"])
                    latencies.append((time.perf_counter() - start) * 1000)

                avg_latency = statistics.mean(latencies)
                results.append((target, avg_latency))

            print("\n" + "=" * 60)
            print("  Query Latency vs Data Volume")
            print("=" * 60)
            for total, latency in results:
                bar = "#" * int(latency * 2)
                print(f"  {total:>6,} docs: {latency:>8.2f} ms {bar}")


# =============================================================================
# CONCURRENCY TESTS
# =============================================================================


class TestConcurrency:
    """Concurrency tests - parallel access patterns."""

    def test_concurrent_writes(self, conn_conf, test_graph_name, clean_db):
        """Concurrent writes from multiple threads."""
        _ = clean_db
        num_threads = 10
        ops_per_thread = 100

        errors = []
        latencies = []
        lock = threading.Lock()

        def writer(thread_id):
            """Write documents from a single thread, recording latencies."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for i in range(ops_per_thread):
                        start = time.perf_counter()
                        db.upsert_docs_batch(
                            [{"id": f"{thread_id}_{i}", "thread": thread_id}],
                            "ConcurrentWrite",
                            match_keys=["id"],
                        )
                        with lock:
                            latencies.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                with lock:
                    errors.append(str(e))

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(writer, i) for i in range(num_threads)]
            concurrent.futures.wait(futures)
        duration = time.perf_counter() - start

        total_ops = num_threads * ops_per_thread
        ops_per_sec = total_ops / duration

        print("\n" + "=" * 60)
        print(f"  Concurrent Writes ({num_threads} threads)")
        print("=" * 60)
        print(f"  Total ops:     {total_ops:,}")
        print(f"  Duration:      {duration:.2f}s")
        print(f"  Throughput:    {ops_per_sec:,.0f} ops/sec")
        print(f"  Avg latency:   {statistics.mean(latencies):.2f} ms")
        print(f"  P95 latency:   {percentile(latencies, 95):.2f} ms")
        print(f"  Errors:        {len(errors)}")

        assert len(errors) == 0, f"Errors: {errors[:5]}"

        # Verify data integrity
        with ConnectionManager(connection_config=conn_conf) as db:
            count = db.aggregate("ConcurrentWrite", AggregationType.COUNT)
            assert count == total_ops

    def test_concurrent_reads(self, conn_conf, test_graph_name, clean_db):
        """Concurrent reads from multiple threads."""
        _ = clean_db
        num_threads = 20
        reads_per_thread = 100

        # Setup test data
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i), "data": "x" * 100} for i in range(1000)]
            db.upsert_docs_batch(docs, "ConcurrentRead", match_keys=["id"])

        errors = []
        latencies = []
        lock = threading.Lock()

        def reader(thread_id):
            """Read documents from a single thread, recording latencies."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for i in range(reads_per_thread):
                        start = time.perf_counter()
                        db.fetch_docs("ConcurrentRead", limit=50)
                        with lock:
                            latencies.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                with lock:
                    errors.append(str(e))

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(reader, i) for i in range(num_threads)]
            concurrent.futures.wait(futures)
        duration = time.perf_counter() - start

        total_ops = num_threads * reads_per_thread
        ops_per_sec = total_ops / duration

        print("\n" + "=" * 60)
        print(f"  Concurrent Reads ({num_threads} threads)")
        print("=" * 60)
        print(f"  Total ops:     {total_ops:,}")
        print(f"  Duration:      {duration:.2f}s")
        print(f"  Throughput:    {ops_per_sec:,.0f} ops/sec")
        print(f"  Avg latency:   {statistics.mean(latencies):.2f} ms")
        print(f"  P95 latency:   {percentile(latencies, 95):.2f} ms")
        print(f"  Errors:        {len(errors)}")

        assert len(errors) == 0

    @pytest.mark.slow
    def test_mixed_read_write_load(self, conn_conf, test_graph_name, clean_db):
        """Mixed concurrent read/write workload."""
        _ = clean_db
        num_writers = 5
        num_readers = 10
        ops_per_thread = 100

        write_latencies = []
        read_latencies = []
        errors = []
        lock = threading.Lock()
        write_counter = [0]

        # Pre-populate
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i), "data": "initial"} for i in range(500)]
            db.upsert_docs_batch(docs, "MixedLoad", match_keys=["id"])

        def writer():
            """Write new documents while recording latencies."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for _ in range(ops_per_thread):
                        with lock:
                            write_counter[0] += 1
                            wid = write_counter[0]
                        start = time.perf_counter()
                        db.upsert_docs_batch(
                            [{"id": f"new_{wid}", "data": "written"}],
                            "MixedLoad",
                            match_keys=["id"],
                        )
                        with lock:
                            write_latencies.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                with lock:
                    errors.append(f"Writer: {e}")

        def reader():
            """Read documents while recording latencies."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for _ in range(ops_per_thread):
                        start = time.perf_counter()
                        db.fetch_docs("MixedLoad", limit=50)
                        with lock:
                            read_latencies.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                with lock:
                    errors.append(f"Reader: {e}")

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_writers + num_readers
        ) as executor:
            futures = []
            futures.extend([executor.submit(writer) for _ in range(num_writers)])
            futures.extend([executor.submit(reader) for _ in range(num_readers)])
            concurrent.futures.wait(futures)
        duration = time.perf_counter() - start

        print("\n" + "=" * 60)
        print(f"  Mixed Load ({num_writers}W/{num_readers}R threads)")
        print("=" * 60)
        print(f"  Duration:      {duration:.2f}s")
        print(f"  Write ops:     {len(write_latencies):,}")
        print(f"  Read ops:      {len(read_latencies):,}")
        print(f"  Write avg:     {statistics.mean(write_latencies):.2f} ms")
        print(f"  Read avg:      {statistics.mean(read_latencies):.2f} ms")
        print(f"  Errors:        {len(errors)}")

        assert len(errors) == 0


# =============================================================================
# BATCH SIZING TESTS
# =============================================================================


class TestBatchSizing:
    """Tests to find optimal batch size."""

    def test_optimal_batch_size(self, conn_conf, test_graph_name, clean_db):
        """Compare different batch sizes for optimal throughput."""
        _ = clean_db
        total_docs = 5000
        batch_sizes = [1, 10, 50, 100, 250, 500, 1000]

        results = []

        for batch_size in batch_sizes:
            # Clean graph before each batch size test
            with ConnectionManager(connection_config=conn_conf) as db:
                db.delete_graph_structure(delete_all=True)

            with ConnectionManager(connection_config=conn_conf) as db:
                num_batches = total_docs // batch_size

                start = time.perf_counter()
                for b in range(num_batches):
                    start_id = b * batch_size
                    docs = [
                        {"id": str(start_id + i), "data": f"v{start_id + i}"}
                        for i in range(batch_size)
                    ]
                    db.upsert_docs_batch(docs, "BatchSize", match_keys=["id"])
                duration = time.perf_counter() - start

                docs_per_sec = total_docs / duration
                results.append((batch_size, docs_per_sec, duration))

        print("\n" + "=" * 60)
        print(f"  Batch Size Optimization ({total_docs:,} docs)")
        print("=" * 60)
        print(f"  {'Size':>6} | {'docs/sec':>10} | {'time':>8}")
        print("  " + "-" * 32)

        best_size, best_rate = 0, 0
        for size, rate, dur in results:
            marker = " *" if rate > best_rate else ""
            if rate > best_rate:
                best_size, best_rate = size, rate
            print(f"  {size:>6} | {rate:>10,.0f} | {dur:>7.2f}s{marker}")

        print(f"\n  Optimal batch size: {best_size}")

    def test_batch_size_memory_impact(self, conn_conf, test_graph_name, clean_db):
        """Measure memory impact of different batch sizes."""
        _ = clean_db
        import sys

        batch_sizes = [100, 500, 1000, 5000]
        doc_size = 1000  # 1KB per doc

        print("\n" + "=" * 60)
        print("  Batch Size Memory Impact")
        print("=" * 60)

        for batch_size in batch_sizes:
            # Create batch in memory
            gc.collect()
            docs = [{"id": str(i), "data": "x" * doc_size} for i in range(batch_size)]
            batch_mem = sys.getsizeof(docs) + sum(sys.getsizeof(d) for d in docs)

            print(f"  Batch {batch_size:>5}: ~{batch_mem / 1024:.1f} KB in memory")

            # Insert batch
            with ConnectionManager(connection_config=conn_conf) as db:
                start = time.perf_counter()
                db.upsert_docs_batch(docs, "MemTest", match_keys=["id"])
                duration = time.perf_counter() - start
                print(f"           Insert time: {duration * 1000:.1f} ms")

            # Cleanup
            with ConnectionManager(connection_config=conn_conf) as db:
                db.delete_graph_structure(delete_all=True)


# =============================================================================
# SUSTAINED LOAD TESTS
# =============================================================================


class TestSustainedLoad:
    """Sustained load tests - stability over time."""

    @pytest.mark.slow
    def test_sustained_write_load(self, conn_conf, test_graph_name, clean_db):
        """Sustained write load over multiple seconds."""
        _ = clean_db
        duration_sec = 10
        batch_size = 50

        latencies = []
        ops_count = 0
        errors = []

        with ConnectionManager(connection_config=conn_conf) as db:
            start_time = time.perf_counter()
            while time.perf_counter() - start_time < duration_sec:
                try:
                    docs = [
                        {"id": str(ops_count * batch_size + i), "ts": time.time()}
                        for i in range(batch_size)
                    ]
                    op_start = time.perf_counter()
                    db.upsert_docs_batch(docs, "Sustained", match_keys=["id"])
                    latencies.append((time.perf_counter() - op_start) * 1000)
                    ops_count += 1
                except Exception as e:
                    errors.append(str(e))

        total_docs = ops_count * batch_size
        actual_duration = time.perf_counter() - start_time

        # Analyze latency stability by time window
        window_size = len(latencies) // 5 if len(latencies) >= 5 else len(latencies)
        windows = [
            latencies[i : i + window_size]
            for i in range(0, len(latencies), window_size)
        ]
        window_avgs = [statistics.mean(w) for w in windows if w]

        print("\n" + "=" * 60)
        print(f"  Sustained Write Load ({duration_sec}s)")
        print("=" * 60)
        print(f"  Total docs:    {total_docs:,}")
        print(f"  Throughput:    {total_docs / actual_duration:,.0f} docs/sec")
        print(f"  Avg latency:   {statistics.mean(latencies):.2f} ms")
        print(f"  P99 latency:   {percentile(latencies, 99):.2f} ms")
        print(f"  Errors:        {len(errors)}")
        print("\n  Latency stability (by time window):")
        for i, avg in enumerate(window_avgs):
            bar = "#" * int(avg * 5)
            print(f"    Window {i + 1}: {avg:.2f} ms {bar}")

        # Document latency stability
        if len(window_avgs) >= 2:
            degradation = window_avgs[-1] / window_avgs[0] if window_avgs[0] > 0 else 1
            print(f"\n  Degradation factor: {degradation:.2f}x")
            if degradation > 5.0:
                print("  WARNING: Unstable latency under sustained load")

    @pytest.mark.slow
    def test_sustained_mixed_load(self, conn_conf, test_graph_name, clean_db):
        """Sustained mixed load with reads and writes."""
        _ = clean_db
        duration_sec = 10

        # Pre-populate
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i), "data": "initial"} for i in range(1000)]
            db.upsert_docs_batch(docs, "SustainedMixed", match_keys=["id"])

        write_ops = [0]
        read_ops = [0]
        errors = []
        running = [True]

        def writer():
            """Continuously write documents until stopped."""
            with ConnectionManager(connection_config=conn_conf) as db:
                while running[0]:
                    try:
                        write_ops[0] += 1
                        db.upsert_docs_batch(
                            [{"id": f"w_{write_ops[0]}", "ts": time.time()}],
                            "SustainedMixed",
                            match_keys=["id"],
                        )
                    except Exception as e:
                        errors.append(f"W: {e}")

        def reader():
            """Continuously read documents until stopped."""
            with ConnectionManager(connection_config=conn_conf) as db:
                while running[0]:
                    try:
                        db.fetch_docs("SustainedMixed", limit=50)
                        read_ops[0] += 1
                    except Exception as e:
                        errors.append(f"R: {e}")

        # Launch threads
        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]

        start = time.perf_counter()
        for t in threads:
            t.start()

        time.sleep(duration_sec)
        running[0] = False

        for t in threads:
            t.join(timeout=5)

        actual_duration = time.perf_counter() - start

        print("\n" + "=" * 60)
        print(f"  Sustained Mixed Load ({duration_sec}s, 2W/3R)")
        print("=" * 60)
        print(
            f"  Write ops:     {write_ops[0]:,} ({write_ops[0] / actual_duration:.0f}/s)"
        )
        print(
            f"  Read ops:      {read_ops[0]:,} ({read_ops[0] / actual_duration:.0f}/s)"
        )
        print(f"  Total ops:     {write_ops[0] + read_ops[0]:,}")
        print(f"  Errors:        {len(errors)}")

        assert len(errors) == 0


# =============================================================================
# LIMITS TESTS
# =============================================================================


class TestLimits:
    """System limits tests."""

    def test_max_property_count(self, conn_conf, test_graph_name, clean_db):
        """Find maximum property count per node."""
        _ = clean_db
        property_counts = [10, 50, 100, 500, 1000, 2000]
        results = []

        for prop_count in property_counts:
            with ConnectionManager(connection_config=conn_conf) as db:
                db.delete_graph_structure(delete_all=True)

                doc = {"id": "test"}
                for i in range(prop_count):
                    doc[f"prop_{i}"] = f"value_{i}"

                try:
                    start = time.perf_counter()
                    db.upsert_docs_batch([doc], "PropLimit", match_keys=["id"])
                    duration = (time.perf_counter() - start) * 1000
                    results.append((prop_count, duration, "OK"))
                except Exception as e:
                    results.append((prop_count, 0, f"FAIL: {str(e)[:30]}"))
                    break

        print("\n" + "=" * 60)
        print("  Max Properties per Node")
        print("=" * 60)
        for count, dur, status in results:
            print(f"  {count:>5} props: {dur:>8.1f} ms - {status}")

    def test_max_batch_size(self, conn_conf, test_graph_name, clean_db):
        """Find practical maximum batch size."""
        _ = clean_db
        batch_sizes = [1000, 5000, 10000]
        results = []

        for size in batch_sizes:
            with ConnectionManager(connection_config=conn_conf) as db:
                db.delete_graph_structure(delete_all=True)

                docs = [{"id": str(i), "data": "x" * 100} for i in range(size)]

                try:
                    start = time.perf_counter()
                    db.upsert_docs_batch(docs, "BatchLimit", match_keys=["id"])
                    duration = time.perf_counter() - start
                    docs_per_sec = size / duration
                    results.append((size, duration, docs_per_sec, "OK"))
                except Exception as e:
                    results.append((size, 0, 0, f"FAIL: {str(e)[:30]}"))
                    break

        print("\n" + "=" * 60)
        print("  Max Batch Size")
        print("=" * 60)
        for size, dur, rate, status in results:
            if status == "OK":
                print(f"  {size:>6}: {dur:>6.2f}s ({rate:>8,.0f} docs/s) - {status}")
            else:
                print(f"  {size:>6}: {status}")

    def test_connection_pool_stress(self, conn_conf, test_graph_name, clean_db):
        """Connection pool stress test."""
        _ = clean_db
        num_connections = 50
        ops_per_connection = 20

        errors = []
        successful = [0]
        lock = threading.Lock()

        def connection_test(conn_id):
            """Open a connection and perform multiple operations."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for i in range(ops_per_connection):
                        db.upsert_docs_batch(
                            [{"id": f"{conn_id}_{i}"}],
                            "PoolStress",
                            match_keys=["id"],
                        )
                    with lock:
                        successful[0] += 1
            except Exception as e:
                with lock:
                    errors.append(f"Conn {conn_id}: {e}")

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_connections
        ) as executor:
            futures = [
                executor.submit(connection_test, i) for i in range(num_connections)
            ]
            concurrent.futures.wait(futures)
        duration = time.perf_counter() - start

        print("\n" + "=" * 60)
        print(f"  Connection Pool Stress ({num_connections} connections)")
        print("=" * 60)
        print(f"  Successful:    {successful[0]}/{num_connections}")
        print(f"  Duration:      {duration:.2f}s")
        print(f"  Errors:        {len(errors)}")

        if errors:
            print(f"  First error:   {errors[0][:50]}")

        # At least 90% success rate
        assert successful[0] >= num_connections * 0.9


# =============================================================================
# GRAPH OPERATIONS TESTS
# =============================================================================


class TestGraphOperationsLoad:
    """Graph-specific operation load tests."""

    def test_edge_creation_throughput(self, conn_conf, test_graph_name, clean_db):
        """Measure edge creation throughput."""
        _ = clean_db
        num_nodes = 500
        num_edges = 2000

        with ConnectionManager(connection_config=conn_conf) as db:
            # Create nodes
            nodes = [{"id": str(i)} for i in range(num_nodes)]
            db.upsert_docs_batch(nodes, "EdgeNode", match_keys=["id"])

            # Create edges with random connections
            import random

            edges = [
                [
                    {"id": str(random.randint(0, num_nodes - 1))},
                    {"id": str(random.randint(0, num_nodes - 1))},
                    {"weight": random.random()},
                ]
                for _ in range(num_edges)
            ]

            # Batch insert edges
            batch_size = 100
            latencies = []

            for i in range(0, len(edges), batch_size):
                batch = edges[i : i + batch_size]
                start = time.perf_counter()
                db.insert_edges_batch(
                    batch,
                    source_class="EdgeNode",
                    target_class="EdgeNode",
                    relation_name="CONNECTS",
                    match_keys_source=["id"],
                    match_keys_target=["id"],
                )
                latencies.append((time.perf_counter() - start) * 1000)

        total_time = sum(latencies) / 1000
        edges_per_sec = num_edges / total_time

        print("\n" + "=" * 60)
        print("  Edge Creation Throughput")
        print("=" * 60)
        print(f"  Nodes:         {num_nodes:,}")
        print(f"  Edges:         {num_edges:,}")
        print(f"  Throughput:    {edges_per_sec:,.0f} edges/sec")
        print(f"  Avg latency:   {statistics.mean(latencies):.2f} ms/batch")

    def test_traversal_performance(self, conn_conf, test_graph_name, clean_db):
        """Measure traversal performance at various depths."""
        _ = clean_db

        with ConnectionManager(connection_config=conn_conf) as db:
            # Create a tree-structured graph
            depth = 5
            branching = 3
            nodes = []
            edges = []

            def create_tree(parent_id, current_depth):
                """Recursively create tree nodes and edges."""
                if current_depth >= depth:
                    return
                for i in range(branching):
                    child_id = f"{parent_id}_{i}"
                    nodes.append({"id": child_id, "depth": current_depth})
                    edges.append([{"id": parent_id}, {"id": child_id}, {}])
                    create_tree(child_id, current_depth + 1)

            nodes.append({"id": "root", "depth": 0})
            create_tree("root", 1)

            db.upsert_docs_batch(nodes, "TreeNode", match_keys=["id"])

            # Insert edges in batches
            batch_size = 100
            for i in range(0, len(edges), batch_size):
                db.insert_edges_batch(
                    edges[i : i + batch_size],
                    source_class="TreeNode",
                    target_class="TreeNode",
                    relation_name="CHILD",
                    match_keys_source=["id"],
                    match_keys_target=["id"],
                )

            print(f"\n  Tree: {len(nodes)} nodes, {len(edges)} edges")

            # Test traversal at different depths
            traversal_depths = [1, 2, 3, 4, 5]
            results = []

            for d in traversal_depths:
                latencies = []
                for _ in range(20):
                    start = time.perf_counter()
                    result = db.execute(
                        f"MATCH (root:TreeNode {{id: 'root'}})-[:CHILD*1..{d}]->(n) RETURN count(n)"
                    )
                    latencies.append((time.perf_counter() - start) * 1000)
                results.append((d, statistics.mean(latencies), result.result_set[0][0]))

            print("\n" + "=" * 60)
            print("  Traversal Performance")
            print("=" * 60)
            for d, lat, count in results:
                print(f"  Depth {d}: {lat:>8.2f} ms ({count:>5} nodes)")
