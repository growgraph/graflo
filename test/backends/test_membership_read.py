"""A derived keys field is read by ``ANY_IN`` through FalkorDB's ``fetch_docs``, guarded.

No database: a recording ``execute`` stands in for the driver.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from graflo.architecture.schema import Schema
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.lookup import _Reader

SCHEMA: dict[str, Any] = {
    "metadata": {"name": "sites", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Unit",
                    "properties": [
                        "serial",
                        {
                            "name": "site",
                            "match": {
                                "normalizer": "label_v1",
                                "parts": {"split": [","], "order": "general_first"},
                            },
                        },
                        {
                            "name": "site_keys",
                            "type": "LIST",
                            "item_type": "STRING",
                            "derived": {"source": "site", "form": "keys"},
                        },
                        {
                            "name": "access_zones",
                            "type": "LIST",
                            "item_type": "INT",
                            "reserved": True,
                        },
                    ],
                    "identity": ["serial"],
                }
            ]
        },
        "edge_config": {"edges": []},
    },
}


class _Falkor(FalkordbConnection):
    def __init__(self, rows: list[list[Any]]) -> None:
        self.rows = rows
        self.sent: list[tuple[str, dict[str, Any]]] = []

    def execute(self, query: str, **params: Any) -> Any:
        self.sent.append((query, params))
        return SimpleNamespace(result_set=self.rows)


def test_falkordb_reads_a_keys_field_by_membership_under_the_guard():
    guard = FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[[1]],
    )
    conn = _Falkor([["KP-1", ["berlin", "berlin hall 2"]]])
    reader = _Reader(conn, Schema.model_validate(SCHEMA), guard)

    found = reader.members("Unit", "site_keys", ["berlin", "lyon"], ["serial"])

    assert found == [{"serial": "KP-1", "site_keys": ["berlin", "berlin hall 2"]}]
    ((query, params),) = conn.sent
    flat = " ".join(query.split())
    assert "any(_any_in IN coalesce(n.site_keys, []) WHERE _any_in IN $" in flat
    assert "any(_any_in IN coalesce(n.access_zones, []) WHERE _any_in IN $" in flat
    assert sorted(map(str, params.values())) == ["['berlin', 'lyon']", "[1]"]
