"""Tests for API data source implementation."""

import asyncio
import logging
from os.path import dirname, realpath

import pytest

from graflo.hq.caster import Caster
from graflo.data_source import (
    APIConfig,
    APIDataSource,
    DataSourceFactory,
    PaginationConfig,
)

logger = logging.getLogger(__name__)


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


@pytest.fixture(scope="function")
def api_mode():
    return "kg"  # Use kg schema for API test


def test_api_data_source_basic(
    mock_api_server, api_mode, schema_obj, current_path, reset
):
    """Test basic API data source functionality."""
    server, port = mock_api_server
    resource_name = api_mode.split("_")[0]
    schema = schema_obj(api_mode)

    # Create API config
    api_config = APIConfig(
        url=f"http://localhost:{port}/api/users",
        method="GET",
        pagination=PaginationConfig(
            strategy="offset",
            offset_param="offset",
            limit_param="limit",
            page_size=2,
            has_more_path="has_more",
            data_path="data",
        ),
    )

    # Create API data source
    api_source = DataSourceFactory.create_api_data_source(api_config)
    api_source.resource_name = resource_name

    # Create caster and process
    caster = Caster(schema, n_cores=1)
    asyncio.run(
        caster.process_data_source(data_source=api_source, resource_name=resource_name)
    )

    # Verify we got data
    # Note: This is a basic test - full verification would require database connection
    assert api_source is not None


def test_api_data_source_via_process_resource(
    mock_api_server, api_mode, schema_obj, current_path, reset
):
    """Test API data source via process_resource with config dict."""
    server, port = mock_api_server
    resource_name = api_mode.split("_")[0]
    schema = schema_obj(api_mode)

    # Create caster
    caster = Caster(schema, n_cores=1)

    # Process using configuration dict
    resource_config = {
        "source_type": "api",
        "config": {
            "url": f"http://localhost:{port}/api/users",
            "method": "GET",
            "pagination": {
                "strategy": "offset",
                "offset_param": "offset",
                "limit_param": "limit",
                "page_size": 2,
                "has_more_path": "has_more",
                "data_path": "data",
            },
        },
    }

    asyncio.run(
        caster.process_resource(
            resource_instance=resource_config,
            resource_name=resource_name,
        )
    )

    # Test passes if no exceptions are raised
    assert True


def test_api_data_source_iter_batches(mock_api_server):
    """Test API data source batch iteration."""
    server, port = mock_api_server

    api_config = APIConfig(
        url=f"http://localhost:{port}/api/users",
        method="GET",
        pagination=PaginationConfig(
            strategy="offset",
            offset_param="offset",
            limit_param="limit",
            page_size=2,
            has_more_path="has_more",
            data_path="data",
        ),
    )

    api_source = APIDataSource(config=api_config)

    # Collect all batches
    all_items = []
    for batch in api_source.iter_batches(batch_size=1, limit=None):
        all_items.extend(batch)

    # Should get all 3 items
    assert len(all_items) == 3
    assert all_items[0]["id"] == 1
    assert all_items[1]["id"] == 2
    assert all_items[2]["id"] == 3
