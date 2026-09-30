from pathlib import Path
from typing import Any, cast

import pytest

from graflo.architecture.schema import Schema
from graflo.connections.onto import DBConfig
from graflo.migrate.executor import MigrationExecutionError, MigrationExecutor
from graflo.migrate.io import schema_hash
from graflo.migrate.models import (
    MigrationOperation,
    MigrationPlan,
    MigrationRecord,
    OperationType,
    RiskLevel,
)
from graflo.migrate.store import FileMigrationStore


def _schema() -> Schema:
    return Schema.from_dict(
        {
            "metadata": {"name": "kg", "version": "1.0.0"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": ["id", "name"],
                            "identity": ["id"],
                        },
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {},
        }
    )


def _arango_config() -> DBConfig:
    return DBConfig.from_dict(
        {
            "connection_type": "arango",
            "uri": "http://localhost:8529",
            "username": "root",
            "password": "root",
            "database": "kg",
        }
    )


def test_file_store_roundtrip(tmp_path: Path):
    store = FileMigrationStore(tmp_path / "migrations.json")
    assert store.history() == []

    record = MigrationRecord(
        revision="0001",
        schema_hash="abc",
        backend="arango",
        operations=["ADD_VERTEX"],
    )
    store.add_record(record)

    assert store.has_revision("0001", "arango")
    assert store.has_schema_hash("abc", "arango")
    assert store.latest("arango") is not None


def test_executor_dry_run_does_not_require_live_db(tmp_path: Path):
    store = FileMigrationStore(tmp_path / "migrations.json")
    executor = MigrationExecutor(store=store)
    schema = _schema()
    plan = MigrationPlan(
        operations=[
            MigrationOperation(
                op_type=OperationType.ADD_VERTEX,
                target="vertex:person",
                risk=RiskLevel.LOW,
            )
        ]
    )

    report = executor.execute_plan(
        revision="0001",
        schema_hash=schema_hash(schema),
        target_schema=schema,
        plan=plan,
        conn_conf=_arango_config(),
        dry_run=True,
    )
    assert report.applied
    assert store.history() == []


def test_executor_blocks_high_risk_when_not_allowed(tmp_path: Path):
    store = FileMigrationStore(tmp_path / "migrations.json")
    executor = MigrationExecutor(store=store, allow_high_risk=False)
    schema = _schema()
    plan = MigrationPlan(
        operations=[
            MigrationOperation(
                op_type=OperationType.REMOVE_VERTEX_FIELD,
                target="vertex:person:field:name",
                risk=RiskLevel.HIGH,
            )
        ]
    )

    with pytest.raises(MigrationExecutionError):
        executor.execute_plan(
            revision="0002",
            schema_hash=schema_hash(schema),
            target_schema=schema,
            plan=plan,
            conn_conf=_arango_config(),
            dry_run=True,
        )


def test_executor_hash_mismatch_on_existing_revision(tmp_path: Path):
    store = FileMigrationStore(tmp_path / "migrations.json")
    executor = MigrationExecutor(store=store)
    schema = _schema()
    plan = MigrationPlan(
        operations=[
            MigrationOperation(
                op_type=OperationType.ADD_VERTEX,
                target="vertex:person",
                risk=RiskLevel.LOW,
            )
        ]
    )
    store.add_record(
        MigrationRecord(
            revision="0001",
            schema_hash="known_hash",
            backend="arango",
            operations=[],
            reversible=True,
            applied_at="2026-01-01T00:00:00+00:00",
        )
    )

    with pytest.raises(MigrationExecutionError):
        executor.execute_plan(
            revision="0001",
            schema_hash=schema_hash(schema),
            target_schema=schema,
            plan=plan,
            conn_conf=_arango_config(),
            dry_run=True,
        )


class _RecordingEmitter:
    def supports(self, operation: MigrationOperation) -> bool:
        return True

    def execute(self, conn, operation: MigrationOperation, *, target_schema) -> str:
        return f"applied {operation.op_type}"


class _NoConnection:
    def __init__(self, **_kwargs) -> None:
        pass

    def __enter__(self) -> object:
        return object()

    def __exit__(self, *_exc) -> None:
        return None


def test_an_applied_migration_records_its_operations_in_full(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from graflo.migrate import executor as executor_module
    from graflo.onto import DBType

    monkeypatch.setattr(executor_module, "ConnectionManager", _NoConnection)
    store = FileMigrationStore(tmp_path / "migrations.json")
    executor = MigrationExecutor(store=store)
    executor._emitters[DBType.ARANGO] = cast(Any, _RecordingEmitter())
    operation = MigrationOperation(
        op_type=OperationType.ADD_VERTEX_FIELD,
        target="vertex:person:field:age",
        new_value={"name": "age", "type": "INT"},
        risk=RiskLevel.LOW,
    )
    schema = _schema()

    executor.execute_plan(
        revision="0003",
        schema_hash=schema_hash(schema),
        target_schema=schema,
        plan=MigrationPlan(operations=[operation]),
        conn_conf=_arango_config(),
        dry_run=False,
    )

    (record,) = FileMigrationStore(tmp_path / "migrations.json").history()
    assert record.operations == [operation]


def test_a_record_holding_bare_operation_types_still_loads(tmp_path: Path):
    path = tmp_path / "migrations.json"
    path.write_text(
        '{"records": [{"revision": "0001", "schema_hash": "abc", '
        '"backend": "arango", "operations": ["ADD_VERTEX"]}]}',
        encoding="utf-8",
    )

    (record,) = FileMigrationStore(path).history()

    assert record.operations == [OperationType.ADD_VERTEX]
