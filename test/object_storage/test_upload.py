"""Tests for upload_staged_csvs (mocked boto3 client)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from graflo.hq.connection_provider import S3GeneralizedConnConfig
from graflo.object_storage.upload import upload_staged_csvs


def test_upload_staged_csvs_paths_and_urls(tmp_path: Path) -> None:
    f1 = tmp_path / "a.csv"
    f1.write_text("x", encoding="utf-8")

    s3_cfg = S3GeneralizedConnConfig(
        bucket=None,
        region="us-east-1",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="k",
        aws_secret_access_key="s",
    )

    mock_client = MagicMock()

    with patch(
        "graflo.object_storage.upload.boto3_s3_client_from_generalized",
        return_value=mock_client,
    ):
        urls = upload_staged_csvs(
            staged_files={"v:Company": f1},
            bucket="myb",
            key_prefix="demo",
            session_id="sess1",
            s3_cfg=s3_cfg,
        )

    mock_client.upload_file.assert_called_once()
    args, _kwargs = mock_client.upload_file.call_args
    assert str(f1) == args[0]
    assert args[1] == "myb"
    assert args[2] == "demo/sess1/a.csv"

    assert urls["v:Company"] == "s3://myb/demo/sess1/a.csv"
