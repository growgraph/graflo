"""Conformance suite for :meth:`Connection.resolve_vertices` across all backends.

Locating an edge endpoint by a *secondary identity* means mapping an arbitrary
field-set back to the vertex's primary identity. Every backend must agree on
that contract — in particular on **multiplicity**, since the caller's ambiguity
policy is driven by how many vertices a key matched.

Each backend is set up in its own namespace and torn down afterwards, so this
module never leaves artifacts behind for other suites to trip over.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from graflo.architecture.schema import Schema
from graflo.db.manager import ConnectionManager
from test.db.backends import ALL_BACKENDS, backend_params, config_for

PROBE_VERTEX = "GfResolveProbe"

SCHEMA_DICT: dict[str, Any] = {
    "metadata": {"name": "gf_resolve_probe", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": PROBE_VERTEX,
                    "properties": [
                        {"name": "id", "type": "STRING"},
                        {"name": "isin", "type": "STRING"},
                        {"name": "org", "type": "STRING"},
                        {"name": "code", "type": "STRING"},
                    ],
                    "identity": ["id"],
                }
            ]
        },
        "edge_config": {"edges": []},
    },
}

#: ``A2`` and ``A3`` deliberately share an ``isin`` so multi-match is exercised.
SEED_DOCS: list[dict[str, str]] = [
    {"id": "A1", "isin": "US001", "org": "acme", "code": "x1"},
    {"id": "A2", "isin": "US002", "org": "globex", "code": "x2"},
    {"id": "A3", "isin": "US002", "org": "acme", "code": "x3"},
]


def _probe_schema() -> Schema:
    return Schema.model_validate(SCHEMA_DICT)


# ── backend configuration ────────────────────────────────────────────────

#: Every backend, including the file backend — endpoint resolution is the one
#: capability all of them must answer, whatever their storage model.
BACKENDS: list[Any] = backend_params(list(ALL_BACKENDS))

PROBE_SPACE = "gf_resolve_probe"


@pytest.fixture(scope="module")
def probe_db(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Any]:
    """Seeded connection for one backend; skips when the backend is unreachable."""
    schema = _probe_schema()
    try:
        config = config_for(
            request.param, space=PROBE_SPACE, tmp_path_factory=tmp_path_factory
        )
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{request.param} config unavailable: {error}")

    try:
        manager = ConnectionManager(connection_config=config)
        db = manager.__enter__()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"backend unreachable: {error}")

    try:
        db.init_db(schema, recreate_schema=True)
        db.upsert_docs_batch(list(SEED_DOCS), PROBE_VERTEX, ["id"])
    except Exception as error:  # pragma: no cover - environment dependent
        manager.__exit__(None, None, None)
        pytest.skip(f"backend setup failed: {error}")

    try:
        yield db
    finally:
        try:
            db.delete_graph_structure(vertex_types=(PROBE_VERTEX,), delete_all=False)
        except Exception:
            pass
        manager.__exit__(None, None, None)


def _ids(resolved: dict[int, list[dict[str, Any]]]) -> dict[int, list[str]]:
    return {
        key: sorted(str(doc["id"]) for doc in docs) for key, docs in resolved.items()
    }


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_single_match(probe_db) -> None:
    """A unary secondary key resolves to the vertex's primary identity."""
    resolved = probe_db.resolve_vertices(
        PROBE_VERTEX, [{"isin": "US001"}], ("isin",), ("id",)
    )
    assert _ids(resolved) == {0: ["A1"]}


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_preserves_multiplicity(probe_db) -> None:
    """Every match is returned — the ambiguity policy needs the count, not the first."""
    resolved = probe_db.resolve_vertices(
        PROBE_VERTEX, [{"isin": "US002"}], ("isin",), ("id",)
    )
    assert _ids(resolved) == {0: ["A2", "A3"]}


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_absent_key_is_omitted(probe_db) -> None:
    """A key matching nothing yields no entry rather than an empty list."""
    resolved = probe_db.resolve_vertices(
        PROBE_VERTEX, [{"isin": "NOPE"}], ("isin",), ("id",)
    )
    assert resolved == {}


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_partial_key_is_omitted(probe_db) -> None:
    """A partial composite key must never be partially matched."""
    resolved = probe_db.resolve_vertices(
        PROBE_VERTEX,
        [{"org": "acme"}, {"org": "acme", "code": "x3"}],
        ("org", "code"),
        ("id",),
    )
    assert _ids(resolved) == {1: ["A3"]}


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_composite_key(probe_db) -> None:
    """Composite secondary keys match on the full field-set."""
    resolved = probe_db.resolve_vertices(
        PROBE_VERTEX,
        [{"org": "acme", "code": "x1"}, {"org": "globex", "code": "x1"}],
        ("org", "code"),
        ("id",),
    )
    assert _ids(resolved) == {0: ["A1"]}


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_positions_map_to_input_order(probe_db) -> None:
    """Results are keyed by position in the input batch, duplicates included."""
    resolved = probe_db.resolve_vertices(
        PROBE_VERTEX,
        [{"isin": "US001"}, {"isin": "NOPE"}, {"isin": "US002"}, {"isin": "US001"}],
        ("isin",),
        ("id",),
    )
    assert _ids(resolved) == {0: ["A1"], 2: ["A2", "A3"], 3: ["A1"]}


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_chunking_is_transparent(probe_db) -> None:
    """Chunking splits the lookup without changing the result."""
    key_docs = [{"isin": "US001"}, {"isin": "US002"}]
    unchunked = probe_db.resolve_vertices(PROBE_VERTEX, key_docs, ("isin",), ("id",))
    chunked = probe_db.resolve_vertices(
        PROBE_VERTEX, key_docs, ("isin",), ("id",), chunk_size=1
    )
    assert _ids(chunked) == _ids(unchunked) == {0: ["A1"], 1: ["A2", "A3"]}


@pytest.mark.parametrize("probe_db", BACKENDS, indirect=True)
def test_resolve_empty_inputs(probe_db) -> None:
    """Empty batches and empty key-sets short-circuit without querying."""
    assert probe_db.resolve_vertices(PROBE_VERTEX, [], ("isin",), ("id",)) == {}
    assert (
        probe_db.resolve_vertices(PROBE_VERTEX, [{"isin": "US001"}], (), ("id",)) == {}
    )
