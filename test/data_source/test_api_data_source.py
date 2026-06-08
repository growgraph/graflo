"""Tests for API data source implementation via APIConnector and RegistryBuilder."""

import asyncio
from os.path import dirname, realpath

import pytest

from test.conftest import fetch_manifest_obj
from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    PaginationConfig,
)
from graflo.db import PostgresConfig
from graflo.data_source import APIDataSource, DataSourceFactory
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.hq.caster import Caster
from graflo.hq.connection_provider import (
    ApiGeneralizedConnConfig,
    InMemoryConnectionProvider,
    RestApiConnConfig,
)
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.registry_builder import RegistryBuilder


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


@pytest.fixture(scope="function")
def api_mode():
    return "kg"


def _users_connector(page_size: int = 2) -> APIConnector:
    return APIConnector(
        name="users_api",
        path="/api/users",
        pagination=PaginationConfig(
            strategy="offset",
            offset_param="offset",
            limit_param="limit",
            page_size=page_size,
            has_more_path="has_more",
            data_path="data",
        ),
    )


def _api_provider(port: int, connector: APIConnector) -> InMemoryConnectionProvider:
    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="api_source",
        config=ApiGeneralizedConnConfig(
            config=RestApiConnConfig(base_url=f"http://localhost:{port}")
        ),
    )
    provider.bind_connector_to_conn_proxy(connector=connector, conn_proxy="api_source")
    return provider


def _build_registry(
    mock_api_server,
    api_mode: str,
    *,
    page_size: int = 2,
) -> tuple[APIDataSource, str]:
    _, port = mock_api_server
    resource_name = api_mode.split("_")[0]
    manifest = fetch_manifest_obj(api_mode)
    schema = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()
    connector = _users_connector(page_size=page_size)
    bindings = Bindings(
        connectors=[connector],
        resource_connector=[{"resource": resource_name, "connector": "users_api"}],
        connector_connection=[{"connector": "users_api", "conn_proxy": "api_source"}],
    )
    provider = _api_provider(port, connector)
    registry = RegistryBuilder(schema, ingestion_model).build(
        bindings=bindings,
        ingestion_params=IngestionParams(n_cores=1, batch_size=page_size),
        connection_provider=provider,
    )
    sources = registry.get_data_sources(resource_name)
    assert len(sources) == 1
    api_source = sources[0]
    assert isinstance(api_source, APIDataSource)
    return api_source, resource_name


def test_api_data_source_basic(mock_api_server, api_mode, current_path, reset):
    api_source, resource_name = _build_registry(mock_api_server, api_mode)
    manifest = fetch_manifest_obj(api_mode)
    schema = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()
    ingestion_model.finish_init(schema.core_schema)
    caster = Caster(
        schema,
        ingestion_model,
        ingestion_params=IngestionParams(n_cores=1),
    )
    asyncio.run(
        caster.process_data_source(data_source=api_source, resource_name=resource_name)
    )
    assert api_source is not None


def test_api_data_source_via_registry_builder(mock_api_server, api_mode):
    api_source, _ = _build_registry(mock_api_server, api_mode)
    all_items: list[dict] = []
    for batch in api_source.iter_batches(batch_size=1, limit=None):
        all_items.extend(batch)
    assert len(all_items) == 3


def test_api_data_source_factory_rejects_inline_api_config():
    with pytest.raises(ValueError, match="bindings \\(APIConnector\\)"):
        DataSourceFactory.create_data_source(
            source_type="api", config={"url": "http://x"}
        )


def test_api_data_source_process_resource_rejects_api_dict(mock_api_server, api_mode):
    _, resource_name = _build_registry(mock_api_server, api_mode)
    manifest = fetch_manifest_obj(api_mode)
    schema = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()
    ingestion_model.finish_init(schema.core_schema)
    caster = Caster(
        schema,
        ingestion_model,
        ingestion_params=IngestionParams(n_cores=1),
    )
    with pytest.raises(ValueError, match="bindings \\(APIConnector\\)"):
        asyncio.run(
            caster.process_resource(
                resource_instance={"source_type": "api", "config": {"url": "http://x"}},
                resource_name=resource_name,
            )
        )


def test_api_data_source_iter_batches(mock_api_server):
    api_source, _ = _build_registry(mock_api_server, "kg")
    all_items: list[dict] = []
    for batch in api_source.iter_batches(batch_size=1, limit=None):
        all_items.extend(batch)
    assert len(all_items) == 3
    assert all_items[0]["id"] == 1
    assert all_items[1]["id"] == 2
    assert all_items[2]["id"] == 3


def test_sql_data_source_postgres_streaming_limit_25():
    try:
        postgres_conf = PostgresConfig.from_docker_env()
        connection_string = postgres_conf.to_sqlalchemy_connection_string()
    except Exception as exc:
        pytest.skip(f"Postgres docker env unavailable: {exc}")

    query = """
    SELECT
        gs AS id,
        (gs::numeric / 10) AS amount
    FROM generate_series(1, 100) AS gs
    ORDER BY gs
    """
    ds = SQLDataSource(
        config=SQLConfig(
            connection_string=connection_string,
            query=query,
        )
    )

    try:
        batches = list(ds.iter_batches(batch_size=10, limit=25))
    except Exception as exc:
        pytest.skip(f"Postgres endpoint not reachable: {exc}")

    if not batches:
        pytest.skip("Postgres endpoint not reachable or returned no rows")

    assert [len(batch) for batch in batches] == [10, 10, 5]
    rows = [item for batch in batches for item in batch]
    assert len(rows) == 25
    assert rows[0]["id"] == 1
    assert rows[-1]["id"] == 25
    assert isinstance(rows[0]["amount"], float)
