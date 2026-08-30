"""``SCHEMA_DDL`` must describe the executor, and refuse the rest by name.

Two facts are worth pinning. The capability is declared on ``Connection``
subclasses while the emitters live in a private dict on the executor, so the
two can drift — and a backend that claims DDL support without an emitter fails
deep inside the executor rather than at the door. And a backend without one
must be refused *before* a connection is opened, so a caller can map it to a
501 rather than relaying a driver error it cannot interpret.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from graflo.architecture.schema import Schema
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.connections.mapping import get_config_class
from graflo.connections.onto import DBConfig
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager
from graflo.migrate.executor import MigrationExecutionError, MigrationExecutor
from graflo.migrate.models import MigrationPlan
from graflo.migrate.store import FileMigrationStore
from graflo.onto import DBType


def _executor(tmp_path: Path) -> MigrationExecutor:
    """Executor whose history store is scoped to the test.

    ``FileMigrationStore`` defaults to ``.graflo/migrations.json`` relative to
    the working directory, so a bare ``MigrationExecutor()`` writes into the
    repository root.
    """
    return MigrationExecutor(store=FileMigrationStore(tmp_path / "migrations.json"))


#: Flavors with an emitter, read once at import for the parametrize below.
_EMITTER_FLAVORS = frozenset(
    ConnectionManager.flavors_supporting(ConnectionCapability.SCHEMA_DDL)
)

#: Never reached — the refusal happens before the schema is consulted.
_MINIMAL_SCHEMA = Schema.model_validate(
    {
        "metadata": {"name": "ddl_capability", "version": "1.0.0"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "probe",
                        "properties": [{"name": "id", "type": "STRING"}],
                        "identity": ["id"],
                    }
                ]
            },
            "edge_config": {"edges": []},
        },
    }
)


def test_declared_capability_matches_the_emitter_registry(tmp_path: Path) -> None:
    """Every backend declaring SCHEMA_DDL has an emitter, and vice versa."""
    declared = set(
        ConnectionManager.flavors_supporting(ConnectionCapability.SCHEMA_DDL)
    )
    registered = set(_executor(tmp_path)._emitters)
    assert declared == registered, (
        f"declared={sorted(f.value for f in declared)} "
        f"registered={sorted(f.value for f in registered)}"
    )


def test_the_capability_is_not_claimed_by_every_backend() -> None:
    """Guards the assertion above against passing because both sides are empty."""
    declared = ConnectionManager.flavors_supporting(ConnectionCapability.SCHEMA_DDL)
    assert declared, "no backend declares SCHEMA_DDL"
    assert len(declared) < len(ConnectionManager.target_conn_mapping)


#: Ids are prefixed because ``pytest_collection_modifyitems`` gates on marker
#: names appearing in ``item.keywords`` — which includes the parametrize id. A
#: param called plainly "nebula" would be skipped without ``--run-nebula``,
#: even though nothing here opens a connection.
@pytest.mark.parametrize(
    "flavor",
    [
        pytest.param(f, id=f"no-emitter-{f.value}")
        for f in ConnectionManager.target_conn_mapping
        if f not in _EMITTER_FLAVORS
    ],
)
def test_unsupported_backend_is_refused_by_name(flavor: DBType, tmp_path: Path) -> None:
    """No connection is opened, and the message names the backend and the alternatives.

    The config is real but deliberately unreachable — nothing here points at a
    live server, which is the whole assertion: the refusal must happen before
    a connection is attempted.
    """
    config: DBConfig
    if flavor is DBType.GRAFLO_BACKEND:
        config = GraFloBackendConfig(output_dir=tmp_path)
    else:
        config = get_config_class(flavor)()

    with pytest.raises(MigrationExecutionError) as excinfo:
        _executor(tmp_path).execute_plan(
            revision="r1",
            schema_hash="h1",
            target_schema=_MINIMAL_SCHEMA,
            plan=MigrationPlan(),
            conn_conf=config,
            dry_run=False,
        )
    message = str(excinfo.value)
    assert flavor.value in message
    assert "arango" in message and "neo4j" in message
