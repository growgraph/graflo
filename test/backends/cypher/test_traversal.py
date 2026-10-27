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

# work_order -targets-> machine <-uses- line <-depends_on- product
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
                for name in ("work_order", "machine", "line", "product")
            ]
        },
        "edge_config": {
            "edges": [
                {"source": "work_order", "target": "machine", "relation": "targets"},
                {"source": "line", "target": "machine", "relation": "uses"},
                {"source": "product", "target": "line", "relation": "depends_on"},
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
        "vertex_type": "work_order",
        "key": "WO1",
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
    _neighbors(schema, run, key={"name": "work order one"})

    query, params = run.calls[0]
    assert "{`name`: $anchor_id}" in query
    assert params == {ANCHOR_PARAM: "work order one"}


def test_one_hop_fans_out_per_relation_touching_the_anchor(schema: Schema) -> None:
    run = _Recorder()
    _neighbors(schema, run, vertex_type="machine", key="press", hops=1)

    queries = [query for query, _ in run.calls]
    assert len(queries) == 2
    assert any(":targets" in q and "(far:work_order)" in q for q in queries)
    assert any(":uses" in q and "(far:line)" in q for q in queries)


def test_several_hops_walk_every_relation_in_one_pattern(schema: Schema) -> None:
    run = _Recorder(
        rows=[
            {"far": {"key": "press"}, "labels": ["machine"]},
            {"far": {"key": "stamping"}, "labels": ["line"]},
            {"far": {"key": "bracket"}, "labels": ["product"]},
            {"far": {"key": "ghost"}, "labels": ["Unmapped"]},
        ]
    )
    container = _neighbors(schema, run, hops=3)

    (query, _), *rest = run.calls
    assert rest == []
    assert "[r:depends_on|targets|uses*1..3]" in query
    assert "(far)" in query
    assert "WHERE far <> anchor" in query
    assert {
        name: [d["key"] for d in docs] for name, docs in container.vertices.items()
    } == {
        "machine": ["press"],
        "line": ["stamping"],
        "product": ["bracket"],
    }


def test_several_hops_honour_the_relation_filter(schema: Schema) -> None:
    run = _Recorder()
    _neighbors(schema, run, hops=2, edge_types=["targets", "uses"])

    (query, _), *_ = run.calls
    assert "[r:targets|uses*1..2]" in query


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


# An undirected edge next to a directed one: peer -knows- peer, peer -owns-> item
_MIXED: dict[str, Any] = {
    "metadata": {"name": "mixed", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": name,
                    "properties": [{"name": "key", "type": "STRING"}],
                    "identity": ["key"],
                }
                for name in ("peer", "item")
            ]
        },
        "edge_config": {
            "edges": [
                {
                    "source": "peer",
                    "target": "peer",
                    "relation": "knows",
                    "directed": False,
                },
                {"source": "peer", "target": "item", "relation": "owns"},
            ],
            "symmetric": ["knows"],
        },
    },
}


@pytest.fixture
def mixed_schema() -> Schema:
    manifest = GraphManifest.model_validate({"schema": _MIXED})
    manifest.finish_init()
    return manifest.require_schema()


def test_an_undirected_edge_is_matched_without_an_arrow(mixed_schema: Schema) -> None:
    """The override follows the same per-edge rule as the backend-neutral default."""
    run = _Recorder()
    _neighbors(
        mixed_schema,
        run,
        vertex_type="peer",
        key="p1",
        hops=1,
        direction=EdgeDirection.OUT,
    )

    by_relation = {
        relation: query
        for query, _ in run.calls
        for relation in ("knows", "owns")
        if f":{relation}" in query
    }
    assert "-[r:knows*1..1]-(far" in by_relation["knows"]
    assert "-[r:owns*1..1]->(far" in by_relation["owns"]


def test_relations_that_disagree_on_direction_are_walked_hop_by_hop(
    mixed_schema: Schema,
) -> None:
    """One variable-length pattern has one direction; mixed relations cannot share it."""
    run = _Recorder(rows=[{"far": {"key": "p2"}, "labels": ["peer"]}])
    container = _neighbors(
        mixed_schema,
        run,
        vertex_type="peer",
        key="p1",
        hops=2,
        direction=EdgeDirection.OUT,
    )

    queries = [query for query, _ in run.calls]
    assert all("*1..1]" in query for query in queries)
    assert any("-[r:knows*1..1]-(far" in query for query in queries)
    assert any("-[r:owns*1..1]->(far" in query for query in queries)
    # The second hop is anchored at the vertex the first hop reached.
    assert {ANCHOR_PARAM: "p2"} in [params for _, params in run.calls]
    assert container.vertices["peer"] == [{"key": "p2"}]


def test_relations_that_agree_still_share_one_pattern(mixed_schema: Schema) -> None:
    run = _Recorder()
    _neighbors(
        mixed_schema,
        run,
        vertex_type="peer",
        key="p1",
        hops=3,
        direction=EdgeDirection.ANY,
    )

    (query,) = [query for query, _ in run.calls]
    assert "-[r:knows|owns*1..3]-" in query


# The mixed shape again, keyed by a composite identity that shares its first field.
_COMPOSITE: dict[str, Any] = {
    "metadata": {"name": "composite", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": name,
                    "properties": [
                        {"name": "region", "type": "STRING"},
                        {"name": "key", "type": "STRING"},
                    ],
                    "identity": ["region", "key"],
                }
                for name in ("peer", "item")
            ]
        },
        "edge_config": _MIXED["graph"]["edge_config"],
    },
}


@pytest.fixture
def composite_schema() -> Schema:
    manifest = GraphManifest.model_validate({"schema": _COMPOSITE})
    manifest.finish_init()
    return manifest.require_schema()


def test_a_composite_anchor_binds_every_field(composite_schema: Schema) -> None:
    run = _Recorder()
    _neighbors(
        composite_schema,
        run,
        vertex_type="peer",
        key={"region": "eu", "key": "p1"},
        hops=1,
    )

    for query, params in run.calls:
        assert "{`region`: $anchor_id, `key`: $anchor_id_1}" in query
        assert params == {ANCHOR_PARAM: "eu", f"{ANCHOR_PARAM}_1": "p1"}


def test_a_hop_by_hop_walk_anchors_each_hop_on_the_full_identity(
    composite_schema: Schema,
) -> None:
    """Two peers share a region: neither may stand in for the other."""
    run = _Recorder(
        rows=[
            {"far": {"region": "eu", "key": "p2"}, "labels": ["peer"]},
            {"far": {"region": "eu", "key": "p3"}, "labels": ["peer"]},
        ]
    )
    container = _neighbors(
        composite_schema,
        run,
        vertex_type="peer",
        key={"region": "eu", "key": "p1"},
        hops=2,
        direction=EdgeDirection.OUT,
    )

    sent = [params for _, params in run.calls]
    for key in ("p2", "p3"):
        assert {ANCHOR_PARAM: "eu", f"{ANCHOR_PARAM}_1": key} in sent
    assert sorted(doc["key"] for doc in container.vertices["peer"]) == ["p2", "p3"]


def _rel(
    relation: str, start: tuple[str, str], end: tuple[str, str], **props: Any
) -> dict[str, Any]:
    """A relationship as the neighbourhood query returns it."""
    return {
        "type": relation,
        "properties": props,
        "start": {"key": start[1]},
        "start_labels": [start[0]],
        "end": {"key": end[1]},
        "end_labels": [end[0]],
    }


def test_the_query_returns_each_reached_node_once_with_its_relationships() -> None:
    query = cypher_neighbors_query(
        anchor_label="node",
        edge_type="links",
        far_label=None,
        direction=EdgeDirection.OUT,
        hops=2,
        limit=5,
    )
    assert "UNWIND relationships(path) AS _r" in query
    assert "collect(DISTINCT _r)" in query
    assert "RETURN DISTINCT" not in query
    assert query.rstrip().endswith("LIMIT 5")


def test_one_hop_returns_the_edges_as_the_default_walk_does(schema: Schema) -> None:
    """Rows keyed by the edge id, endpoints under ``source`` and ``target``."""
    run = _Recorder(
        rows=[
            {
                "far": {"key": "press"},
                "labels": ["machine"],
                "edges": [
                    _rel("targets", ("work_order", "WO1"), ("machine", "press"), w=1)
                ],
            }
        ]
    )
    container = _neighbors(schema, run, direction=EdgeDirection.OUT)

    assert container.edges == {
        ("work_order", "machine", "targets"): [
            {"w": 1, "source": "WO1", "target": "press"}
        ]
    }


def test_several_hops_return_every_relationship_of_the_paths_once(
    schema: Schema,
) -> None:
    targets = _rel("targets", ("work_order", "WO1"), ("machine", "press"))
    uses = _rel("uses", ("line", "stamping"), ("machine", "press"))
    run = _Recorder(
        rows=[
            {"far": {"key": "press"}, "labels": ["machine"], "edges": [targets]},
            {"far": {"key": "stamping"}, "labels": ["line"], "edges": [targets, uses]},
        ]
    )
    container = _neighbors(schema, run, hops=2)

    assert container.edges == {
        ("work_order", "machine", "targets"): [{"source": "WO1", "target": "press"}],
        ("line", "machine", "uses"): [{"source": "stamping", "target": "press"}],
    }


def test_a_relationship_of_an_unselected_relation_is_left_out(schema: Schema) -> None:
    run = _Recorder(
        rows=[
            {
                "far": {"key": "press"},
                "labels": ["machine"],
                "edges": [_rel("uses", ("line", "stamping"), ("machine", "press"))],
            }
        ]
    )
    container = _neighbors(schema, run, edge_types=["targets"])

    assert container.edges == {}


@pytest.fixture
def inverse_schema() -> Schema:
    spec = {
        **SCHEMA,
        "graph": {
            **SCHEMA["graph"],
            "edge_config": {
                **SCHEMA["graph"]["edge_config"],
                "inverses": [{"relation": "targets", "inverse": "targeted_by"}],
            },
        },
    }
    manifest = GraphManifest.model_validate({"schema": spec})
    manifest.finish_init()
    return manifest.require_schema()


def test_a_declared_inverse_reports_the_edge_under_its_name(
    inverse_schema: Schema,
) -> None:
    """Read backwards, endpoints in that reading's order, as the default walk."""
    run = _Recorder(
        rows=[
            {
                "far": {"key": "WO1"},
                "labels": ["work_order"],
                "edges": [_rel("targets", ("work_order", "WO1"), ("machine", "press"))],
            }
        ]
    )
    container = _neighbors(
        inverse_schema,
        run,
        vertex_type="machine",
        key="press",
        edge_types=["targeted_by"],
        direction=EdgeDirection.OUT,
    )

    assert container.edges == {
        ("machine", "work_order", "targeted_by"): [{"source": "press", "target": "WO1"}]
    }


def test_the_limit_caps_the_edge_rows(schema: Schema) -> None:
    """``limit`` is the most edges a neighbourhood holds, on every backend."""
    path = [
        _rel("targets", ("work_order", "WO1"), ("machine", "press")),
        _rel("uses", ("line", "stamping"), ("machine", "press")),
        _rel("depends_on", ("product", "bracket"), ("line", "stamping")),
    ]
    run = _Recorder(
        rows=[{"far": {"key": "bracket"}, "labels": ["product"], "edges": path}]
    )
    container = _neighbors(schema, run, hops=3, limit=1)

    assert sum(len(rows) for rows in container.edges.values()) == 1
