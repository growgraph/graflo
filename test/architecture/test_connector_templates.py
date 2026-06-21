from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    PaginationConfig,
    PaginationRequestConfig,
)


def test_template_base_scalar_override() -> None:
    bindings = Bindings.model_validate(
        {
            "connector_templates": [
                {
                    "name": "api_base",
                    "path": "/api/query",
                    "method": "GET",
                }
            ],
            "connectors": [
                {
                    "name": "custom",
                    "base": "api_base",
                    "path": "/api/custom",
                }
            ],
        }
    )
    connector = bindings.connectors[0]
    assert isinstance(connector, APIConnector)
    assert connector.path == "/api/custom"
    assert connector.method == "GET"


def test_template_base_dict_merge_params() -> None:
    bindings = Bindings.model_validate(
        {
            "connector_templates": [
                {
                    "name": "api_base",
                    "path": "/api/query",
                    "params": {"tenant": "acme", "version": "1"},
                }
            ],
            "connectors": [
                {
                    "name": "users",
                    "base": "api_base",
                    "params": {"query": "show USER"},
                }
            ],
        }
    )
    connector = bindings.connectors[0]
    assert isinstance(connector, APIConnector)
    assert connector.params == {
        "tenant": "acme",
        "version": "1",
        "query": "show USER",
    }


def test_template_base_dict_merge_row_annotations() -> None:
    bindings = Bindings.model_validate(
        {
            "connector_templates": [
                {
                    "name": "api_base",
                    "path": "/api/query",
                    "row_annotations": {"_rel": "RelationC"},
                }
            ],
            "connectors": [
                {
                    "name": "edge_ab",
                    "base": "api_base",
                    "row_annotations": {"_src_type": "TypeA", "_tgt_type": "TypeB"},
                }
            ],
        }
    )
    connector = bindings.connectors[0]
    assert isinstance(connector, APIConnector)
    assert connector.row_annotations == {
        "_rel": "RelationC",
        "_src_type": "TypeA",
        "_tgt_type": "TypeB",
    }


def test_template_base_pagination_replaced() -> None:
    bindings = Bindings.model_validate(
        {
            "connector_templates": [
                {
                    "name": "api_base",
                    "path": "/api/query",
                    "pagination": {
                        "request": {
                            "strategy": "offset",
                            "page_size": 100,
                        },
                    },
                }
            ],
            "connectors": [
                {
                    "name": "small_pages",
                    "base": "api_base",
                    "pagination": {
                        "request": {
                            "strategy": "offset",
                            "page_size": 25,
                        },
                    },
                }
            ],
        }
    )
    connector = bindings.connectors[0]
    assert isinstance(connector, APIConnector)
    assert connector.pagination is not None
    assert connector.pagination.request.page_size == 25
    assert connector.pagination.request.strategy == "offset"


def test_template_conn_proxy_auto_generates_connector_connection() -> None:
    bindings = Bindings.model_validate(
        {
            "connector_templates": [
                {
                    "name": "api_base",
                    "path": "/api/query",
                    "conn_proxy": "api_source",
                }
            ],
            "connectors": [
                {
                    "name": "users",
                    "base": "api_base",
                }
            ],
        }
    )
    connector = bindings.connectors[0]
    assert isinstance(connector, APIConnector)
    assert bindings.get_conn_proxy_for_connector(connector) == "api_source"


def test_template_conn_proxy_requires_connector_name() -> None:
    with pytest.raises(ValueError, match="must declare 'name'"):
        Bindings.model_validate(
            {
                "connector_templates": [
                    {
                        "name": "api_base",
                        "path": "/api/query",
                        "conn_proxy": "api_source",
                    }
                ],
                "connectors": [{"base": "api_base"}],
            }
        )


def test_template_base_not_found_raises() -> None:
    with pytest.raises(ValueError, match="unknown connector template"):
        Bindings.model_validate(
            {
                "connector_templates": [
                    {"name": "api_base", "path": "/api/query"},
                ],
                "connectors": [{"name": "users", "base": "missing"}],
            }
        )


def test_template_resource_name_propagates() -> None:
    bindings = Bindings.model_validate(
        {
            "connector_templates": [
                {
                    "name": "api_base",
                    "path": "/api/query",
                    "resource_name": "polymorphic_edges",
                }
            ],
            "connectors": [
                {
                    "name": "edge_ab",
                    "base": "api_base",
                    "params": {"query": "show TypeA"},
                }
            ],
        }
    )
    connector = bindings.connectors[0]
    assert isinstance(connector, APIConnector)
    assert connector.resource_name == "polymorphic_edges"
    assert bindings.get_connectors_for_resource("polymorphic_edges") == [connector]


def test_template_connector_connection_not_duplicated() -> None:
    bindings = Bindings.model_validate(
        {
            "connector_templates": [
                {
                    "name": "api_base",
                    "path": "/api/query",
                    "conn_proxy": "api_source",
                }
            ],
            "connectors": [{"name": "users", "base": "api_base"}],
            "connector_connection": [{"connector": "users", "conn_proxy": "other"}],
        }
    )
    connector = bindings.connectors[0]
    assert isinstance(connector, APIConnector)
    assert bindings.get_conn_proxy_for_connector(connector) == "other"
    assert len(bindings.connector_connection) == 1


def test_bindings_default_conn_proxy_applies_to_unmapped_connectors() -> None:
    connector = APIConnector(name="users", path="/api/users")
    bindings = Bindings(
        conn_proxy="api_source",
        connectors=[connector],
    )
    assert bindings.get_conn_proxy_for_connector(connector) == "api_source"


def test_bindings_default_conn_proxy_does_not_override_explicit_mapping() -> None:
    connector = APIConnector(name="users", path="/api/users")
    bindings = Bindings(
        conn_proxy="default_proxy",
        connectors=[connector],
        connector_connection=[{"connector": "users", "conn_proxy": "explicit"}],
    )
    assert bindings.get_conn_proxy_for_connector(connector) == "explicit"


def test_no_connector_templates_noop() -> None:
    from graflo.architecture.contract.bindings import (
        APIConnector,
    )

    connector = APIConnector(
        name="users",
        path="/api/users",
        pagination=PaginationConfig(
            request=PaginationRequestConfig(page_size=50),
        ),
    )
    bindings = Bindings(connectors=[connector])
    assert bindings.connectors == [connector]
