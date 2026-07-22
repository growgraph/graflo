"""Tests for API data source implementation via APIConnector and RegistryBuilder."""

import asyncio
from os.path import dirname, realpath
from unittest.mock import MagicMock, patch

import pytest

from test.conftest import fetch_manifest_obj
from graflo.architecture.contract.bindings import (
    APIConnector,
    ApiResponseStructure,
    Bindings,
    PaginationConfig,
    PaginationRequestConfig,
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
            request=PaginationRequestConfig(
                strategy="offset",
                offset_param="offset",
                limit_param="limit",
                page_size=page_size,
            ),
            response=ApiResponseStructure(
                records_path="data",
                has_more_path="has_more",
            ),
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


def test_api_data_source_iter_batches_respects_total_limit(mock_api_server):
    api_source, _ = _build_registry(mock_api_server, "kg", page_size=2)
    all_items: list[dict] = []
    for batch in api_source.iter_batches(batch_size=1, limit=2):
        all_items.extend(batch)
    assert len(all_items) == 2
    assert all_items[0]["id"] == 1
    assert all_items[1]["id"] == 2


def test_pagination_config_rejects_unknown_strategy():
    with pytest.raises(ValueError):
        PaginationRequestConfig(strategy="unknown")  # ty: ignore[invalid-argument-type]


def test_row_annotations_merged_as_defaults() -> None:
    from graflo.data_source.api import APIConfig

    config = APIConfig(
        url="http://example.com/api",
        row_annotations={"_src_type": "User", "_tgt_type": "Group"},
    )
    source = APIDataSource(config=config)

    response = MagicMock()
    response.json.return_value = [{"id": 1, "name": "alice"}]
    response.raise_for_status.return_value = None

    with patch.object(source, "_create_session") as create_session:
        session = MagicMock()
        session.request.return_value = response
        create_session.return_value = session

        rows = [row for batch in source.iter_batches() for row in batch]

    assert rows == [
        {
            "_src_type": "User",
            "_tgt_type": "Group",
            "id": 1,
            "name": "alice",
        }
    ]


def test_row_annotations_doc_wins_on_collision() -> None:
    from graflo.data_source.api import APIConfig

    config = APIConfig(
        url="http://example.com/api",
        row_annotations={"_src_type": "User"},
    )
    source = APIDataSource(config=config)

    response = MagicMock()
    response.json.return_value = [{"_src_type": "Override", "id": 1}]
    response.raise_for_status.return_value = None

    with patch.object(source, "_create_session") as create_session:
        session = MagicMock()
        session.request.return_value = response
        create_session.return_value = session

        rows = [row for batch in source.iter_batches() for row in batch]

    assert rows == [{"_src_type": "Override", "id": 1}]


def _envelope_connector(page_size: int = 2) -> APIConnector:
    return APIConnector(
        name="items_api",
        path="/api/items",
        pagination=PaginationConfig(
            request=PaginationRequestConfig(
                strategy="offset",
                offset_param="offset",
                limit_param="limit",
                page_size=page_size,
            ),
            response=ApiResponseStructure(
                records_path="results",
                total_count_path="count",
                offset_path="offset",
                next_offset_path="next_offset",
                batch_metadata_paths={"_batch_id": "result_id"},
            ),
        ),
    )


def _build_envelope_registry(
    mock_envelope_api_server,
    *,
    page_size: int = 2,
) -> APIDataSource:
    _, port = mock_envelope_api_server
    connector = _envelope_connector(page_size=page_size)
    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="api_source",
        config=ApiGeneralizedConnConfig(
            config=RestApiConnConfig(base_url=f"http://localhost:{port}")
        ),
    )
    provider.bind_connector_to_conn_proxy(connector=connector, conn_proxy="api_source")
    config = connector.build_api_config(
        base_url=f"http://localhost:{port}",
        page_size_override=page_size,
    )
    return APIDataSource(config=config)


def test_api_data_source_envelope_next_offset(mock_envelope_api_server) -> None:
    _, port = mock_envelope_api_server
    api_source = _build_envelope_registry(mock_envelope_api_server, page_size=2)
    all_items: list[dict] = []
    request_urls: list[str] = []

    session = api_source._create_session()
    original_request = session.request

    def track_request(*args, **kwargs):
        request_urls.append(kwargs.get("url", args[1] if len(args) > 1 else ""))
        return original_request(*args, **kwargs)

    with patch.object(api_source, "_create_session") as create_session:
        create_session.return_value = session
        session.request = track_request  # ty: ignore[invalid-assignment]
        for batch in api_source.iter_batches(batch_size=10, limit=None):
            all_items.extend(batch)

    assert len(all_items) == 3
    assert all(item["_batch_id"].startswith("batch-") for item in all_items)
    assert request_urls == [
        f"http://localhost:{port}/api/items",
        f"http://localhost:{port}/api/items",
    ]


def test_api_data_source_envelope_auto_detect(mock_envelope_api_server) -> None:
    connector = APIConnector(
        name="items_api",
        path="/api/items",
        pagination=PaginationConfig(
            request=PaginationRequestConfig(strategy="offset", page_size=2),
            response=ApiResponseStructure(
                auto_detect=True,
                next_offset_path="next_offset",
            ),
        ),
    )
    _, port = mock_envelope_api_server
    config = connector.build_api_config(base_url=f"http://localhost:{port}")
    api_source = APIDataSource(config=config)
    all_items = [row for batch in api_source.iter_batches() for row in batch]
    assert len(all_items) == 3
    assert all_items[0]["id"] == 1


def test_api_data_source_envelope_without_records_path_raises(
    mock_envelope_api_server,
) -> None:
    connector = APIConnector(
        name="items_api",
        path="/api/items",
        pagination=PaginationConfig(
            request=PaginationRequestConfig(strategy="offset", page_size=2),
            response=ApiResponseStructure(),
        ),
    )
    _, port = mock_envelope_api_server
    config = connector.build_api_config(base_url=f"http://localhost:{port}")
    api_source = APIDataSource(config=config)
    with pytest.raises(ValueError, match="records_path"):
        list(api_source.iter_batches())


def _session_token_response() -> ApiResponseStructure:
    return ApiResponseStructure(
        records_path="0.results",
        next_offset_path="0.next_offset",
        total_count_path="0.count",
        offset_path="0.offset",
    )


def _build_session_token_source(
    mock_session_token_api_server,
    *,
    carry_params: dict[str, str] | None = None,
) -> APIDataSource:
    _, port = mock_session_token_api_server
    request_kwargs: dict = {
        "strategy": "offset",
        "offset_param": "offset",
        "limit_param": None,
        "page_size": 2,
    }
    if carry_params is not None:
        request_kwargs["carry_params"] = carry_params
    connector = APIConnector(
        name="hosts_api",
        path="/api/search",
        pagination=PaginationConfig(
            request=PaginationRequestConfig(**request_kwargs),
            response=_session_token_response(),
        ),
    )
    config = connector.build_api_config(base_url=f"http://localhost:{port}")
    return APIDataSource(config=config)


def test_api_data_source_explicit_carry_params(
    mock_session_token_api_server,
) -> None:
    api_source = _build_session_token_source(
        mock_session_token_api_server,
        carry_params={"results_id": "0.results_id"},
    )
    request_params: list[dict] = []

    session = api_source._create_session()
    original_request = session.request

    def track_request(*args, **kwargs):
        request_params.append(dict(kwargs.get("params") or {}))
        return original_request(*args, **kwargs)

    with patch.object(api_source, "_create_session") as create_session:
        create_session.return_value = session
        session.request = track_request  # ty: ignore[invalid-assignment]
        rows = [row for batch in api_source.iter_batches() for row in batch]

    assert [row["id"] for row in rows] == [1, 2, 3]
    assert "results_id" not in request_params[0]
    assert "limit" not in request_params[0]
    assert request_params[1]["results_id"] == ("SG9zdABuco8EWAIAB9oAAAV84w==")
    assert "limit" not in request_params[1]


def test_api_data_source_auto_detect_carry_params(
    mock_session_token_api_server,
) -> None:
    api_source = _build_session_token_source(mock_session_token_api_server)
    request_params: list[dict] = []

    session = api_source._create_session()
    original_request = session.request

    def track_request(*args, **kwargs):
        request_params.append(dict(kwargs.get("params") or {}))
        return original_request(*args, **kwargs)

    with patch.object(api_source, "_create_session") as create_session:
        create_session.return_value = session
        session.request = track_request  # ty: ignore[invalid-assignment]
        rows = [row for batch in api_source.iter_batches() for row in batch]

    assert len(rows) == 3
    assert "results_id" not in request_params[0]
    assert request_params[1]["results_id"] == ("SG9zdABuco8EWAIAB9oAAAV84w==")


def test_api_data_source_result_id_batch_metadata_not_carried(
    mock_envelope_api_server,
) -> None:
    """Singular result_id is batch metadata, not an auto-detected carry token."""
    api_source = _build_envelope_registry(mock_envelope_api_server, page_size=2)
    request_params: list[dict] = []

    session = api_source._create_session()
    original_request = session.request

    def track_request(*args, **kwargs):
        request_params.append(dict(kwargs.get("params") or {}))
        return original_request(*args, **kwargs)

    with patch.object(api_source, "_create_session") as create_session:
        create_session.return_value = session
        session.request = track_request  # ty: ignore[invalid-assignment]
        rows = [row for batch in api_source.iter_batches() for row in batch]

    assert len(rows) == 3
    assert all(item["_batch_id"].startswith("batch-") for item in rows)
    assert all("results_id" not in params for params in request_params)
    assert all("result_id" not in params for params in request_params)


def test_api_connector_build_api_config_passes_row_annotations() -> None:
    connector = APIConnector(
        path="/api/query",
        row_annotations={"_rel": "RelationC"},
    )
    config = connector.build_api_config(base_url="https://api.example.com")
    assert config.row_annotations == {"_rel": "RelationC"}


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
