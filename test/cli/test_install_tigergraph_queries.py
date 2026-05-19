"""Tests for TigerGraph query install CLI helpers."""

from graflo.cli.install_tigergraph_queries import (
    _gsql_response_indicates_error,
    query_name_from_gsql,
)


def test_query_name_from_create_query() -> None:
    content = "CREATE QUERY countGB() FOR GRAPH public { PRINT 1; }"
    assert query_name_from_gsql(content) == "countGB"


def test_query_name_from_create_or_replace_query() -> None:
    content = "CREATE OR REPLACE QUERY triples() FOR GRAPH accounting { }"
    assert query_name_from_gsql(content) == "triples"


def test_query_name_fallback_to_stem() -> None:
    assert query_name_from_gsql("SELECT 1", fallback="my_query") == "my_query"


def test_gsql_response_error_heuristic() -> None:
    assert _gsql_response_indicates_error("Compilation error: syntax")
    assert not _gsql_response_indicates_error("Successfully created queries")
