"""A populated target must refuse ``apply_target_schema(recreate=False)``.

Every backend raises ``SchemaExistsError`` from that call once the target holds
data, which is what protects a first write from clobbering a populated graph.
It is also why migration currently cannot run against any real database
(``CORE-MIGRATE-001``): the emitters route their DDL through the same call, so
they inherit a guard meant for a different job.

Pinning the behaviour here is what keeps the eventual fix honest. Migration has
to gain its own entry point — ``apply_schema_delta`` — rather than relaxing
this guard, and a suite that never asserted the guard would not notice the
difference.

Uses the shared backend registry, so a new backend inherits this guard by
appearing in ``ALL_BACKENDS`` rather than by anyone remembering to add it.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from graflo.architecture.schema import Schema
from graflo.db.conn import SchemaExistsError
from graflo.db.manager import ConnectionManager
from test.db.backends import ALL_BACKENDS, backend_params, config_for

SPACE = "gf_first_write_guard"
PROBE_VERTEX = "probe"

SCHEMA_DICT: dict[str, Any] = {
    "metadata": {"name": SPACE, "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": PROBE_VERTEX,
                    "properties": [
                        {"name": "id", "type": "STRING"},
                        {"name": "name", "type": "STRING"},
                    ],
                    "identity": ["id"],
                }
            ]
        },
        "edge_config": {"edges": []},
    },
}

SEED_DOCS = [{"id": "a", "name": "alpha"}, {"id": "b", "name": "beta"}]

BACKENDS = backend_params(list(ALL_BACKENDS))


@pytest.fixture(scope="module")
def populated_db(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Any]:
    """A backend holding rows — the defining condition of a migration target."""
    schema = Schema.model_validate(SCHEMA_DICT)
    try:
        config = config_for(
            request.param, space=SPACE, tmp_path_factory=tmp_path_factory
        )
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{request.param} config unavailable: {error}")

    try:
        manager = ConnectionManager(connection_config=config)
        db = manager.__enter__()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{request.param} unreachable: {error}")

    try:
        db.init_db(schema, recreate_schema=True)
        db.upsert_docs_batch(list(SEED_DOCS), PROBE_VERTEX, ["id"])
    except Exception as error:  # pragma: no cover - environment dependent
        manager.__exit__(None, None, None)
        pytest.skip(f"{request.param} setup failed: {error}")

    try:
        yield db, schema
    finally:
        try:
            db.delete_graph_structure(vertex_types=(PROBE_VERTEX,), delete_all=False)
        except Exception:
            pass
        manager.__exit__(None, None, None)


@pytest.mark.parametrize("populated_db", BACKENDS, indirect=True)
def test_apply_target_schema_refuses_a_populated_target(populated_db) -> None:
    db, schema = populated_db
    with pytest.raises(SchemaExistsError):
        db.apply_target_schema(schema, recreate=False)


@pytest.mark.parametrize("populated_db", BACKENDS, indirect=True)
def test_recreate_still_succeeds_on_a_populated_target(populated_db) -> None:
    """The guard is about *first write*, not a lock: recreate=True still applies."""
    db, schema = populated_db
    db.apply_target_schema(schema, recreate=True)
    db.upsert_docs_batch(list(SEED_DOCS), PROBE_VERTEX, ["id"])
