"""The TigerGraph bulk loading job, its row encoding and its response check, offline."""

from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from graflo.architecture.evolution.sanitize import physical_schema
from graflo.architecture.schema import Schema
from graflo.db.bulk import BulkVertexChunk
from graflo.db.tigergraph import rest_client
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.db.tigergraph.gsql_parsers import split_ddl_terms
from graflo.db.tigergraph.loading_job import (
    EOL,
    LIST_SEP,
    SEP,
    EdgeLoad,
    LoadColumn,
    LoadRejectedError,
    VertexLoad,
    check_load_response,
    create_job_gsql,
    edge_line,
    encode_value,
    loading_job,
    split_body,
    vertex_line,
)
from graflo.db.tigergraph.schema_ddl import SchemaDdlBuilder
from graflo.onto import DBType


def _schema() -> Schema:
    """Two edge types share relation ``member_of`` from different source types."""
    schema = Schema.model_validate(
        {
            "metadata": {"name": "g"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Person",
                            "properties": [
                                "pid",
                                "name",
                                {"name": "tags", "type": "LIST", "item_type": "STRING"},
                            ],
                            "identity": ["pid"],
                        },
                        {
                            "name": "Company",
                            "properties": ["cid", "label"],
                            "identity": ["cid"],
                        },
                        {
                            "name": "Seat",
                            "properties": [
                                "hall",
                                {"name": "num", "type": "INT"},
                                {"name": "at", "type": "DATETIME"},
                            ],
                            "identity": ["hall", "num"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "Person",
                            "target": "Company",
                            "relation": "member_of",
                            "properties": ["role"],
                            "identities": [["source", "target", "role"]],
                        },
                        {
                            "source": "Seat",
                            "target": "Company",
                            "relation": "member_of",
                            "properties": [{"name": "since", "type": "INT"}],
                            "identities": [["source", "target", "role"]],
                        },
                    ]
                },
            },
        }
    )
    return physical_schema(schema, DBType.TIGERGRAPH)


GOLDEN = """\
CREATE LOADING JOB graflo_bulk_g FOR GRAPH g {
  DEFINE FILENAME v_Person;
  DEFINE FILENAME v_Company;
  DEFINE FILENAME v_Seat;
  DEFINE FILENAME e_Person__member_of__Company;
  DEFINE FILENAME e_Seat__member_of__Company;
  LOAD v_Person TO VERTEX Person VALUES ($0, $1, SPLIT($2, "<L>")) USING <U>, NEW_VERTEX_ONLY="true";
  LOAD v_Company TO VERTEX Company VALUES ($0, $1) USING <U>, NEW_VERTEX_ONLY="true";
  LOAD v_Seat TO VERTEX Seat VALUES ($0, $1, $2) USING <U>, NEW_VERTEX_ONLY="true";
  LOAD e_Person__member_of__Company TO EDGE member_of VALUES ($0 Person, $1 Company, $2, $3) USING <U>, VERTEX_MUST_EXIST="true";
  LOAD e_Seat__member_of__Company TO EDGE member_of VALUES ($0 Seat, $1 Company, $2, $3) USING <U>, VERTEX_MUST_EXIST="true";
}""".replace("<L>", LIST_SEP).replace(
    "<U>", f'SEPARATOR="{SEP}", EOL="{EOL}", HEADER="false"'
)


def test_edge_types_sharing_a_relation_get_their_own_file_and_load() -> None:
    assert create_job_gsql(loading_job(_schema(), "g")) == GOLDEN


def test_each_endpoint_pair_carries_every_attribute_of_the_type() -> None:
    job = loading_job(_schema(), "g")
    person = job.edge(("Person", "member_of", "Company"))
    seat = job.edge(("Seat", "member_of", "Company"))
    assert person.file_var != seat.file_var
    assert [c.name for c in person.columns] == ["role", "since"]
    assert person.columns == seat.columns


class _Sent(Exception):
    """Stops the DDL once its schema change job is captured."""


def _ddl_statements(schema: Schema) -> list[str]:
    """The ADD VERTEX / ADD EDGE statements the schema DDL runs."""
    sent: list[str] = []
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    stub = cast(Any, conn)
    stub.config = SimpleNamespace(max_job_size=100_000)
    stub._require_configured_graph_name = lambda: "g"

    def execute(command: str) -> str:
        if "CREATE SCHEMA_CHANGE JOB" in command:
            sent.append(command)
            if " EDGE " in command:  # edges are applied after every vertex
                raise _Sent
            return "Local schema change succeeded."
        return ""

    stub._execute_gsql = execute
    with pytest.raises(_Sent):
        SchemaDdlBuilder(conn)._define_schema_local(schema)
    body = "\n".join(sent)
    return re.findall(
        r"ADD (?:DIRECTED |UNDIRECTED )?(?:VERTEX|EDGE) .*?\n\s*\)", body, re.DOTALL
    )


def _declared_attributes(statement: str) -> tuple[str, list[str]]:
    name = statement.split("(", 1)[0].split()[-1]
    body = statement.split("(", 1)[1].rsplit(")", 1)[0]
    names: list[str] = []
    for term in split_ddl_terms(body):
        tokens = term.replace("(", " ( ").split()
        head = tokens[0].upper()
        if head in {"FROM", "TO"} or term.upper().startswith("PRIMARY KEY"):
            continue
        if head == "PRIMARY_ID":
            names.append(tokens[1])
        elif head == "DISCRIMINATOR":
            # Repeated on every FROM/TO pair; declared once.
            inner = term[term.index("(") + 1 : term.rindex(")")]
            names.extend(
                n for n in (t.split()[0] for t in inner.split(",")) if n not in names
            )
        else:
            names.append(tokens[0])
    return name, names


def test_columns_follow_the_attribute_order_the_ddl_declares() -> None:
    schema = _schema()
    job = loading_job(schema, "g")
    declared = dict(_declared_attributes(s) for s in _ddl_statements(schema))
    for v in job.vertices:
        assert [c.name for c in v.columns] == declared[v.vertex_type]
    for e in job.edges:
        assert [c.name for c in e.columns] == declared[e.edge_type]


# -- encoding -------------------------------------------------------------------

STRING = LoadColumn(name="s", gsql_type="STRING")
DATETIME = LoadColumn(name="t", gsql_type="DATETIME")
LIST = LoadColumn(name="l", gsql_type="LIST<STRING>")


@pytest.mark.parametrize(
    ("value", "column", "text"),
    [
        (None, STRING, ""),
        ("", STRING, ""),
        ('say "hi", then\nleave', STRING, 'say "hi", then\nleave'),
        ("ünï", STRING, "ünï"),
        (True, STRING, "true"),
        (False, STRING, "false"),
        (0, STRING, "0"),
        (-12, STRING, "-12"),
        (1.5, STRING, "1.5"),
        (Decimal("2.25"), STRING, "2.25"),
        (UUID(int=1), STRING, "00000000-0000-0000-0000-000000000001"),
        (dt.date(2024, 2, 3), DATETIME, "2024-02-03"),
        (dt.datetime(2024, 2, 3, 4, 5, 6, 789), DATETIME, "2024-02-03 04:05:06"),
        (
            dt.datetime(2024, 2, 3, 4, 5, 6, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            DATETIME,
            "2024-02-03 02:05:06",
        ),
        (dt.datetime(2024, 2, 3, 4, 5, 6), STRING, "2024-02-03T04:05:06"),
        (dt.time(4, 5), STRING, "04:05:00"),
        (["a", None, "b"], LIST, f"a{LIST_SEP}b"),
        ([], LIST, ""),
        ([1, 2], LIST, f"1{LIST_SEP}2"),
    ],
)
def test_values_encode_as_the_loader_reads_them(
    value: Any, column: LoadColumn, text: str
) -> None:
    assert encode_value(value, column) == text


@pytest.mark.parametrize(
    ("value", "column", "error"),
    [
        (f"a{SEP}b", STRING, ValueError),
        (f"a{EOL}b", STRING, ValueError),
        ([f"a{LIST_SEP}b"], LIST, ValueError),
        (float("nan"), STRING, ValueError),
        ({"k": 1}, STRING, TypeError),
        ("a", LIST, TypeError),
    ],
)
def test_values_the_loader_cannot_read_are_refused(
    value: Any, column: LoadColumn, error: type[Exception]
) -> None:
    with pytest.raises(error):
        encode_value(value, column)


def test_a_vertex_row_leaves_absent_and_null_attributes_empty() -> None:
    load = loading_job(_schema(), "g").vertex("Person")
    line = vertex_line(load, {"pid": "p1", "name": None, "extra": "dropped"})
    assert line == f"p1{SEP}{SEP}{EOL}".encode()


def test_a_composite_key_vertex_row_carries_each_key_attribute() -> None:
    load = loading_job(_schema(), "g").vertex("Seat")
    line = vertex_line(
        load, {"hall": "A", "num": 7, "at": dt.datetime(2020, 1, 2, 3, 4, 5)}
    )
    assert line == f"A{SEP}7{SEP}2020-01-02 03:04:05{EOL}".encode()


def test_an_edge_row_names_its_endpoints_as_the_upsert_path_does() -> None:
    load = loading_job(_schema(), "g").edge(("Seat", "member_of", "Company"))
    line = edge_line(
        load,
        ({"hall": "A", "num": 7}, {"cid": "c1"}, {"since": 2001}),
        ["hall", "num"],
        ["cid"],
    )
    assert line == f"A_7{SEP}c1{SEP}{SEP}2001{EOL}".encode()


def test_an_edge_row_without_an_endpoint_identity_is_refused() -> None:
    load = loading_job(_schema(), "g").edge(("Person", "member_of", "Company"))
    with pytest.raises(ValueError, match="no value"):
        edge_line(load, ({}, {"cid": "c1"}, {}), ["pid"], ["cid"])


# -- bodies ---------------------------------------------------------------------


def test_bodies_split_on_row_boundaries_under_the_cap() -> None:
    lines = [f"row{i}{'x' * (i % 7)}{EOL}".encode() for i in range(200)]
    parts = list(split_body(lines, max_bytes=64))
    assert all(len(body) <= 64 for body, _ in parts)
    assert sum(n for _, n in parts) == len(lines)
    assert b"".join(body for body, _ in parts) == b"".join(lines)
    for body, n in parts:
        assert body.endswith(EOL.encode()) and body.count(EOL.encode()) == n


def test_a_row_larger_than_the_cap_is_refused() -> None:
    with pytest.raises(ValueError, match="exceeds"):
        list(split_body([b"x" * 10], max_bytes=9))


def test_an_empty_chunk_posts_nothing() -> None:
    assert list(split_body([])) == []


# -- responses ------------------------------------------------------------------


def _response(**stats: Any) -> dict[str, Any]:
    vertex = {"typeName": "Person", "validObject": stats.pop("validObject", 2)}
    vertex.update(stats.pop("vertex", {}))
    return {
        "version": {"edition": "community"},
        "error": False,
        "message": "",
        "results": [
            {
                "sourceFileName": "Online_POST",
                "statistics": {
                    "validLine": 2,
                    "rejectLine": 0,
                    "failedConditionLine": 0,
                    "notEnoughToken": 0,
                    "invalidJson": 0,
                    "oversizeToken": 0,
                    "vertex": [vertex],
                    "edge": [],
                    **stats,
                },
            }
        ],
    }


def test_a_clean_response_passes() -> None:
    check_load_response(_response(), lines=2)


@pytest.mark.parametrize(
    "stats",
    [
        {"rejectLine": 1, "validLine": 1},
        {"notEnoughToken": 1},
        {"vertex": {"invalidAttribute": 1}},
        {"vertex": {"invalidAttribute": {"count": 1, "lineNumber": [2]}}},
        {"validLine": 1},
    ],
)
def test_a_response_reporting_skipped_lines_raises(stats: dict[str, Any]) -> None:
    with pytest.raises(LoadRejectedError):
        check_load_response(_response(**stats), lines=2)


def test_an_error_response_raises() -> None:
    with pytest.raises(LoadRejectedError, match="no such job"):
        check_load_response({"error": True, "message": "no such job"}, lines=1)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any], status: int = 200) -> None:
        self._payload = payload
        self.status_code = status
        self.ok = status < 400
        self.text = str(payload)

    def json(self) -> dict[str, Any]:
        return self._payload


def test_a_vertex_chunk_is_posted_to_its_file_and_rejected_lines_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    stub = cast(Any, conn)
    stub.config = SimpleNamespace(
        database="g", schema_name="g", username=None, password=None
    )
    stub.restpp_url = "http://tg.test:14240/restpp"
    stub.ssl_verify = True
    stub.api_token = None
    stub._token_cache_key = None
    stub._get_auth_headers = lambda use_basic_auth=False: {}
    posts: list[dict[str, Any]] = []
    reply = {"payload": _response()}

    def post(url: str, **kwargs: Any) -> _FakeResponse:
        posts.append({"url": url, **kwargs})
        return _FakeResponse(reply["payload"])

    monkeypatch.setattr(rest_client.requests, "post", post)
    chunk = BulkVertexChunk(
        chunk_id="v:0",
        class_name="Person",
        match_keys=["pid"],
        rows=[{"pid": "p1", "name": "Ann"}, {"pid": "p2", "tags": ["x"]}],
    )

    conn.bulk_write_vertices(_schema(), chunk)
    (sent,) = posts
    assert sent["url"] == "http://tg.test:14240/restpp/ddl/g"
    assert sent["params"] == {
        "tag": "graflo_bulk_g",
        "filename": "v_Person",
        "sep": SEP,
        "eol": EOL,
    }
    assert sent["data"] == (f"p1{SEP}Ann{SEP}{EOL}p2{SEP}{SEP}x{EOL}".encode())

    reply["payload"] = _response(rejectLine=1, validLine=1)
    with pytest.raises(LoadRejectedError):
        conn.bulk_write_vertices(_schema(), chunk)


def test_vertex_and_edge_loads_are_typed_models() -> None:
    job = loading_job(_schema(), "g")
    assert all(isinstance(v, VertexLoad) for v in job.vertices)
    assert all(isinstance(e, EdgeLoad) for e in job.edges)
