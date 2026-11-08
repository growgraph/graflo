"""Staging then the DuckDB fold equals the reference fold the file backend reads with.

Rows are staged in random batches and folded; the result must equal
:func:`~graflo.architecture.backend.fold.fold_vertices` /
:func:`~graflo.architecture.backend.fold.fold_edges` over the same rows in
``seq`` order. Identities are type-consistent, the domain on which the typed
bulk key and the reference's string key agree.
"""

from __future__ import annotations

import datetime as dt
import itertools
import shutil
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from graflo.architecture.backend.fold import fold_edges, fold_vertices
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.hq.bulk.encode import (
    IdentityKeyError,
    decode,
    dumps,
    encode,
    read_rows,
    typed_key,
    write_rows,
)
from graflo.hq.bulk.fold import FoldCounts, fold, folded_path
from graflo.hq.bulk.stage import Stager
from graflo.hq.write_prepare import WriteContext
from graflo.onto import DBType
from test.hypothesis_budget import examples

UNION = "u"
SCHEMA = Schema.model_validate(
    {
        "metadata": {"name": "fold", "version": "1.0.0"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "V",
                        "properties": [
                            "id",
                            "a",
                            "b",
                            {
                                "name": UNION,
                                "type": "LIST",
                                "item_type": "INT",
                                "merge": "union",
                            },
                        ],
                        "identity": ["id"],
                    }
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": "V",
                        "target": "V",
                        "relation": "r",
                        "properties": [
                            "w",
                            {
                                "name": UNION,
                                "type": "LIST",
                                "item_type": "INT",
                                "merge": "union",
                            },
                        ],
                    }
                ]
            },
        },
        "db_profile": {"db_flavor": "graflo_backend"},
    }
)
EDGE = ("V", "V", "r")

ABSENT = object()
scalars = st.one_of(st.none(), st.integers(-3, 3), st.sampled_from(["x", "y"]))
members = st.one_of(st.none(), st.integers(0, 4))
union_values = st.one_of(st.none(), members, st.lists(members, max_size=4))


def _doc(fields: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in fields.items() if v is not ABSENT}


vertex_rows = st.lists(
    st.builds(
        lambda i, a, b, u: _doc({"id": i, "a": a, "b": b, UNION: u}),
        st.integers(0, 4),
        st.one_of(st.just(ABSENT), scalars),
        st.one_of(st.just(ABSENT), scalars),
        st.one_of(st.just(ABSENT), union_values),
    ),
    min_size=1,
    max_size=25,
)
edge_rows = st.lists(
    st.builds(
        lambda s, t, w, u, x: (
            {"id": s, **({} if x is ABSENT else {"x": x})},
            {"id": t},
            _doc({"w": w, UNION: u}),
        ),
        st.integers(0, 3),
        st.integers(0, 3),
        st.one_of(st.just(ABSENT), scalars),
        st.one_of(st.just(ABSENT), union_values),
        st.one_of(st.just(ABSENT), st.integers(0, 2)),
    ),
    max_size=20,
)
cuts = st.lists(st.integers(0, 25), max_size=4)


def _batches(rows: list, cut: list[int]) -> list[list]:
    bounds = sorted({0, len(rows), *(c for c in cut if c < len(rows))})
    return [rows[a:b] for a, b in itertools.pairwise(bounds)]


def _stage(root: Path, vertices: list[dict], edges: list[tuple], cut: list[int]):
    ctx = WriteContext.for_flavor(SCHEMA, DBType.GRAFLO_BACKEND)
    stager = Stager(SCHEMA, ctx, root)
    # Every endpoint exists, so no edge is rejected here.
    stager.stage(
        GraphContainer(vertices={"V": [{"id": i} for i in range(5)]}), (0, 0, 0, 0)
    )
    for index, batch in enumerate(_batches(vertices, cut), start=1):
        stager.stage(
            GraphContainer(vertices={"V": [dict(d) for d in batch]}), (0, 0, index, 0)
        )
    for index, batch in enumerate(_batches(edges, cut), start=1):
        triples = [(dict(s), dict(t), dict(p)) for s, t, p in batch]
        stager.stage(GraphContainer(edges={EDGE: triples}), (1, 0, index, 0))


def _by(rows: list, key) -> dict:
    return {key(row): row for row in rows}


@settings(max_examples=examples(100))
@given(vertices=vertex_rows, edges=edge_rows, cut=cuts)
def test_the_duckdb_fold_equals_the_reference_fold(vertices, edges, cut) -> None:
    root = Path(tempfile.mkdtemp())
    try:
        _stage(root, vertices, edges, cut)
        counts = fold(root)
        v_run = counts.vertex("V")
        assert v_run is not None
        got_v = [r["doc"] for r in read_rows(folded_path(root, v_run.slot))]
        got_e = [
            [r["source"], r["target"], r["doc"]]
            for run in counts.runs
            if run.type.kind == "edge"
            for r in read_rows(folded_path(root, run.slot))
        ]
    finally:
        shutil.rmtree(root)

    want_v = fold_vertices(
        [{"id": i} for i in range(5)] + [dict(d) for d in vertices], ["id"], [UNION]
    )
    want_e = fold_edges(
        [[dict(s), dict(t), dict(p)] for s, t, p in edges], ["id"], ["id"], [], [UNION]
    )
    assert _by(got_v, lambda d: d["id"]) == _by(want_v, lambda d: d["id"])
    edge_key = lambda e: (e[0]["id"], e[1]["id"])
    assert _by(got_e, edge_key) == _by(want_e, edge_key)
    assert len(got_e) == len(want_e)


def test_an_edge_to_a_vertex_never_staged_is_rejected_and_counted(tmp_path) -> None:
    ctx = WriteContext.for_flavor(SCHEMA, DBType.GRAFLO_BACKEND)
    stager = Stager(SCHEMA, ctx, tmp_path)
    stager.stage(
        GraphContainer(
            vertices={"V": [{"id": 1}]},
            edges={EDGE: [({"id": 1}, {"id": 2}, {}), ({"id": 1}, {"id": 1}, {})]},
        ),
        (0, 0, 0, 0),
    )
    counts = fold(tmp_path)

    (edge_run,) = [r for r in counts.runs if r.type.kind == "edge"]
    assert (edge_run.staged, edge_run.folded, edge_run.rejected) == (2, 1, 1)
    (reject,) = read_rows(tmp_path / "rejects" / f"{edge_run.slot}.endpoints.jsonl.zst")
    assert reject["reason"] == "missing target"
    assert FoldCounts.load(tmp_path) == counts


def test_a_row_with_an_incomplete_identity_is_rejected_at_staging(tmp_path) -> None:
    ctx = WriteContext.for_flavor(SCHEMA, DBType.GRAFLO_BACKEND)
    Stager(SCHEMA, ctx, tmp_path).stage(
        GraphContainer(vertices={"V": [{"id": 1}, {"id": float("nan")}]}), (0, 0, 0, 0)
    )
    (run,) = fold(tmp_path).runs
    assert (run.staged, run.folded, run.rejected) == (1, 1, 1)


def test_dense_ids_number_vertices_across_types_and_reach_edge_endpoints(
    tmp_path,
) -> None:
    ctx = WriteContext.for_flavor(SCHEMA, DBType.GRAFLO_BACKEND)
    Stager(SCHEMA, ctx, tmp_path).stage(
        GraphContainer(
            vertices={"V": [{"id": 2}, {"id": 1}, {"id": 2}]},
            edges={EDGE: [({"id": 2}, {"id": 1}, {})]},
        ),
        (0, 0, 0, 0),
    )
    counts = fold(tmp_path, dense_ids=True)
    v_run = counts.vertex("V")
    assert v_run is not None and v_run.dense == (0, 2)
    dense = {
        r["doc"]["id"]: r["dense"] for r in read_rows(folded_path(tmp_path, v_run.slot))
    }
    (edge_run,) = [r for r in counts.runs if r.type.kind == "edge"]
    (edge,) = read_rows(folded_path(tmp_path, edge_run.slot))
    assert (edge["sdense"], edge["tdense"]) == (dense[2], dense[1])


@pytest.mark.parametrize(
    "value",
    [
        dt.datetime(2026, 11, 7, 12, 30, tzinfo=dt.UTC),
        dt.date(2026, 11, 7),
        dt.time(12, 30),
        UUID(int=7),
        Decimal("1.50"),
        float("inf"),
        {"nested": [1, None, "a"]},
    ],
    ids=str,
)
def test_values_round_trip_through_a_staged_file(tmp_path, value) -> None:
    path = tmp_path / "rows.jsonl.zst"
    write_rows(path, [dumps({"v": encode(value)})])
    assert list(read_rows(path)) == [{"v": value}]
    assert decode(encode(value)) == value


def test_keys_compare_as_the_targets_compare_values() -> None:
    assert typed_key([1]) == typed_key([1.0])
    assert typed_key([1]) != typed_key(["1"])
    assert typed_key([True]) != typed_key([1])
    assert typed_key([UUID(int=1)]) == typed_key([str(UUID(int=1))])
    with pytest.raises(IdentityKeyError):
        typed_key([None])
    with pytest.raises(IdentityKeyError):
        typed_key([""])
    with pytest.raises(IdentityKeyError):
        typed_key([None, ""])


def test_a_composite_identity_keys_on_the_members_it_has() -> None:
    assert typed_key(["x", None]) == '[["s","x"],null]'
    assert typed_key(["x", None]) != typed_key(["x", ""])
    with pytest.raises(IdentityKeyError):
        typed_key(["x", float("nan")])
