"""Tests for TigerGraph query install CLI helpers."""

import pytest

from graflo.cli.install_tigergraph_queries import (
    _gsql_response_indicates_error,
    _tigergraph_config,
    prepare_gsql_content,
    query_name_from_gsql,
    substitute_for_graph_header,
)


def test_query_name_from_create_query() -> None:
    content = "CREATE QUERY countGB() FOR GRAPH public { PRINT 1; }"
    assert query_name_from_gsql(content) == "countGB"


def test_query_name_from_create_or_replace_query() -> None:
    content = "CREATE OR REPLACE QUERY triples() FOR GRAPH accounting { }"
    assert query_name_from_gsql(content) == "triples"


def test_query_name_from_distributed_query() -> None:
    content = (
        "CREATE OR REPLACE DISTRIBUTED QUERY myQuery(INT x) "
        "FOR GRAPH <<GRAPH_NAME>> { }"
    )
    assert query_name_from_gsql(content) == "myQuery"


def test_substitute_for_graph_header_hardcoded_name() -> None:
    template = "CREATE QUERY countGB() FOR GRAPH public { PRINT 1; }"
    prepared, previous = substitute_for_graph_header(template, "my_graph")
    assert previous == ["public"]
    assert "FOR GRAPH my_graph {" in prepared


def test_substitute_for_graph_header_placeholder() -> None:
    template = "CREATE OR REPLACE DISTRIBUTED QUERY q() FOR GRAPH <<GRAPH_NAME>> { }"
    prepared, previous = substitute_for_graph_header(template, "production")
    assert previous == ["<<GRAPH_NAME>>"]
    assert "FOR GRAPH production {" in prepared


def test_substitute_for_graph_header_does_not_touch_body() -> None:
    template = (
        "CREATE OR REPLACE DISTRIBUTED QUERY q() FOR GRAPH template { "
        "USE GRAPH template; }"
    )
    prepared, _ = substitute_for_graph_header(template, "production")
    assert "FOR GRAPH production {" in prepared
    assert "USE GRAPH template;" in prepared


def test_substitute_for_graph_header_whitespace() -> None:
    template = "CREATE QUERY q() FOR GRAPH  accounting  {\n  PRINT 1;\n}"
    prepared, previous = substitute_for_graph_header(template, "target")
    assert previous == ["accounting"]
    assert prepared.startswith("CREATE QUERY q() FOR GRAPH target {")


def test_prepare_gsql_content_raises_without_header() -> None:
    with pytest.raises(ValueError, match="FOR GRAPH"):
        prepare_gsql_content("CREATE QUERY q() { PRINT 1; }", "g")


def test_query_name_fallback_to_stem() -> None:
    assert query_name_from_gsql("SELECT 1", fallback="my_query") == "my_query"


def test_tigergraph_config_cli_overrides_ssl_verify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TIGERGRAPH_URI", "https://localhost:14240")
    monkeypatch.setenv("TIGERGRAPH_SSL_VERIFY", "true")

    config = _tigergraph_config(None, ssl_verify=False)
    assert config.ssl_verify is False

    config = _tigergraph_config(None, ssl_verify=True)
    assert config.ssl_verify is True


def test_gsql_response_error_heuristic() -> None:
    assert _gsql_response_indicates_error("Compilation error: syntax")
    assert not _gsql_response_indicates_error("Successfully created queries")
