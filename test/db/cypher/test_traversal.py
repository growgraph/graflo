"""Cypher neighbourhood queries: parameter binding and cross-relation walks.

No database: a recording ``run`` stands in for the driver, so these pin the
query text a request produces and how rows are shaped into a container.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema import Schema
from graflo.db.cypher.traversal import (
    ANCHOR_PARAM,
    cypher_graph_neighbors,
    cypher_neighbors_query,
)
from graflo.onto import DBType

# change -targets-> server <-runs_on- app <-depends_on- service
SCHEMA: dict[str, Any] = {
    "metadata": {"name": "impact", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": name,
                    "properties": [
                        {"name": "key", "type": "STRING"},
                        {"name": "name", "type": "STRING"},
                    ],
                    "identity": ["key"],
                }
                for name in ("change", "server", "app", "service")
            ]
        },
        "edge_config": {
            "edges": [
                {"source": "change", "target": "server", "relation": "targets"},
                {"source": "app", "target": "server", "relation": "runs_on"},
                {"source": "service", "target": "app", "relation": "depends_on"},
            ]
        },
    },
}


class _Conn:
    """Just enough of a connection for name resolution and addressing."""

    flavor = DBType.NEO4J

    def vertex_address(
        self, doc: dict[str, Any], identity_fields: Sequence[str]
    ) -> str | None:
        for field in identity_fields:
            if doc.get(field) is not None:
                return str(doc[field])
        return None


class _Recorder:
    """Records each query and answers from a canned row list."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.rows = rows or []

    def __call__(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((query, params))
        return self.rows


@pytest.fixture
def schema() -> Schema:
    manifest = GraphManifest.model_validate({"schema": SCHEMA})
    manifest.finish_init()
    return manifest.require_schema()


def _neighbors(schema: Schema, run: _Recorder, **kwargs: Any):
    params: dict[str, Any] = {
        "vertex_type": "change",
        "key": "CHG1",
        "hops": 1,
        "direction": EdgeDirection.ANY,
        "edge_types": None,
        "limit": None,
    }
    params.update(kwargs)
    return cypher_graph_neighbors(_Conn(), schema=schema, run=run, **params)  # ty: ignore[invalid-argument-type]


def test_the_anchor_value_is_bound_not_interpolated(schema: Schema) -> None:
    hostile = "x'}) DETACH DELETE anchor //"
    run = _Recorder()
    _neighbors(schema, run, key=hostile)

    assert run.calls
    for query, params in run.calls:
        assert hostile not in query
        assert f"${ANCHOR_PARAM}" in query
        assert params == {ANCHOR_PARAM: hostile}


def test_a_mapping_key_must_name_a_declared_property(schema: Schema) -> None:
    run = _Recorder()
    with pytest.raises(ValueError, match="not a declared property"):
        _neighbors(schema, run, key={"key}) DETACH DELETE anchor //": "x"})
    assert run.calls == []


def test_a_declared_mapping_key_is_quoted(schema: Schema) -> None:
    run = _Recorder()
    _neighbors(schema, run, key={"name": "change one"})

    query, params = run.calls[0]
    assert "{`name`: $anchor_id}" in query
    assert params == {ANCHOR_PARAM: "change one"}


def test_one_hop_fans_out_per_relation_touching_the_anchor(schema: Schema) -> None:
    run = _Recorder()
    _neighbors(schema, run, vertex_type="server", key="srv", hops=1)

    queries = [query for query, _ in run.calls]
    assert len(queries) == 2
    assert any(":targets" in q and "(far:change)" in q for q in queries)
    assert any(":runs_on" in q and "(far:app)" in q for q in queries)


def test_several_hops_walk_every_relation_in_one_pattern(schema: Schema) -> None:
    run = _Recorder(
        rows=[
            {"far": {"key": "srv"}, "labels": ["server"]},
            {"far": {"key": "api"}, "labels": ["app"]},
            {"far": {"key": "pay"}, "labels": ["service"]},
            {"far": {"key": "ghost"}, "labels": ["Unmapped"]},
        ]
    )
    container = _neighbors(schema, run, hops=3)

    (query, _), *rest = run.calls
    assert rest == []
    assert "[r:depends_on|runs_on|targets*1..3]" in query
    assert "(far)" in query
    assert "WHERE far <> anchor" in query
    assert {
        name: [d["key"] for d in docs] for name, docs in container.vertices.items()
    } == {
        "server": ["srv"],
        "app": ["api"],
        "service": ["pay"],
    }


def test_several_hops_honour_the_relation_filter(schema: Schema) -> None:
    run = _Recorder()
    _neighbors(schema, run, hops=2, edge_types=["targets", "runs_on"])

    (query, _), *_ = run.calls
    assert "[r:runs_on|targets*1..2]" in query


def test_the_query_excludes_the_anchor() -> None:
    query = cypher_neighbors_query(
        anchor_label="node",
        edge_type="links",
        far_label=None,
        direction=EdgeDirection.OUT,
        hops=6,
        limit=None,
    )
    assert "WHERE far <> anchor" in query
