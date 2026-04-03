from pathlib import Path

import pytest

from graflo.architecture.schema import Schema
from graflo.db import DBConfig
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
