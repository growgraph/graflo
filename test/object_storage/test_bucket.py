"""Tests for ensure_bucket_exists (mocked boto3 client)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from graflo.object_storage.bucket import (
    ensure_bucket_exists,
    ensure_staging_bucket_for_config,
)
from graflo.object_storage.config import MinioConfig


def test_ensure_bucket_exists_already_present() -> None:
    client = MagicMock()
    client.head_bucket.return_value = {}

    name = ensure_bucket_exists(client, "b")

    assert name == "b"
    client.head_bucket.assert_called_once_with(Bucket="b")
    client.create_bucket.assert_not_called()


def test_ensure_bucket_exists_creates_when_missing() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        "HeadBucket",
    )

    name = ensure_bucket_exists(client, "newbucket")

    assert name == "newbucket"
    client.create_bucket.assert_called_once_with(Bucket="newbucket")


def test_ensure_bucket_exists_race_bucket_already_owned() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        "HeadBucket",
    )
    client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyOwnedByYou"}},
        "CreateBucket",
    )

    name = ensure_bucket_exists(client, "b")

    assert name == "b"


def test_ensure_bucket_exists_propagates_other_head_errors() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = ClientError(
        {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        },
        "HeadBucket",
    )

    with pytest.raises(ClientError):
        ensure_bucket_exists(client, "b")


def test_ensure_staging_bucket_for_config_wraps_endpoint_connection_error() -> None:
    cfg = MinioConfig(
        endpoint_url="http://127.0.0.1:9000",
        access_key="k",
        secret_key="s",
    )
    mock_client = MagicMock()
    mock_client.head_bucket.side_effect = EndpointConnectionError(
        endpoint_url="http://127.0.0.1:9000/graflo-staging",
        error=ConnectionRefusedError(),
    )
    with patch(
        "graflo.object_storage.s3_client.boto3_s3_client_from_minio",
        return_value=mock_client,
    ):
        with pytest.raises(RuntimeError, match="Cannot connect to"):
            ensure_staging_bucket_for_config(cfg)
