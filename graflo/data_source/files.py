"""Files read through a files connection: a local directory or an S3 prefix.

A ``FileConnector`` bound to a :class:`~graflo.connections.sources.FilesConnConfig`
reads ``sub_path`` relative to the connection's root. :func:`list_files` finds
the files it matches; :func:`file_data_source` reads one of them. Both serve
sampling and ingestion alike.
"""

from __future__ import annotations

import re
import tempfile
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from pydantic import PrivateAttr

from graflo.architecture.graph_types import EncodingType
from graflo.connections.sources import FilesConnConfig, S3ConnConfig
from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.data_source.factory import DataSourceFactory

if TYPE_CHECKING:
    from graflo.architecture.contract.bindings.connectors import FileConnector


class FileEntry:
    """One file under a files connection: its path relative to the root, and size."""

    __slots__ = ("relative", "size")

    def __init__(self, relative: str, size: int | None) -> None:
        self.relative = relative
        self.size = size

    def __repr__(self) -> str:
        return f"FileEntry({self.relative!r}, {self.size!r})"


def _s3_client(files: FilesConnConfig) -> Any:
    from graflo.object_storage.s3_client import boto3_s3_client

    return boto3_s3_client(files.s3_config())


def _s3_key(files: FilesConnConfig, relative: str) -> str:
    return "/".join(part for part in (files.prefix, relative.strip("/")) if part)


def walk(
    files: FilesConnConfig,
    sub_path: str = "",
    *,
    recursive: bool = True,
    limit: int | None = None,
) -> list[FileEntry]:
    """Every file under *sub_path* of *files*, sorted by relative path.

    Args:
        files: The files connection.
        sub_path: Directory relative to the connection's root.
        recursive: Also list the directories below *sub_path*.
        limit: Stop after this many files.
    """
    base = str(PurePosixPath(sub_path)).strip("/")
    base = "" if base == "." else base
    found: list[FileEntry] = []
    if files.is_s3:
        client = _s3_client(files)
        root_key = _s3_key(files, base)
        prefix = f"{root_key}/" if root_key else ""
        kwargs: dict[str, Any] = {"Bucket": files.bucket, "Prefix": prefix}
        if not recursive:
            kwargs["Delimiter"] = "/"
        strip = f"{files.prefix}/" if files.prefix else ""
        for page in client.get_paginator("list_objects_v2").paginate(**kwargs):
            for item in page.get("Contents", []):
                key = str(item["Key"])
                if key.endswith("/"):
                    continue
                found.append(FileEntry(key[len(strip) :], item.get("Size")))
    else:
        root = files.local_root
        directory = root / base if base else root
        if directory.is_dir():
            candidates = directory.rglob("*") if recursive else directory.iterdir()
            found = [
                FileEntry(path.relative_to(root).as_posix(), path.stat().st_size)
                for path in candidates
                if path.is_file()
            ]
    found.sort(key=lambda entry: entry.relative)
    return found[:limit] if limit is not None else found


def list_files(
    files: FilesConnConfig, connector: FileConnector, limit: int | None = None
) -> list[str]:
    """Paths, relative to the root of *files*, of the files *connector* matches.

    ``connector.regex`` matches a file's name, as for a local directory.
    """
    entries = walk(files, str(connector.sub_path), recursive=connector.recursive)
    matched = [
        entry.relative
        for entry in entries
        if connector.regex is None
        or re.search(connector.regex, PurePosixPath(entry.relative).name) is not None
    ]
    return matched[:limit] if limit is not None else matched


class S3ObjectDataSource(AbstractDataSource):
    """One S3 object, read as the file it is: downloaded, then read like a local file.

    The download lives only while :meth:`iter_batches` runs.
    """

    s3: S3ConnConfig
    key: str
    file_type: str | None = None
    encoding: EncodingType = EncodingType.UTF_8
    source_type: DataSourceType = DataSourceType.FILE
    _client: Any = PrivateAttr(default=None)

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        from graflo.object_storage.s3_client import boto3_s3_client

        client = self._client or boto3_s3_client(self.s3)
        name = PurePosixPath(self.key).name
        with tempfile.TemporaryDirectory(prefix="graflo-s3-") as tmp:
            local = Path(tmp) / name
            client.download_file(self.s3.bucket, self.key, str(local))
            source = DataSourceFactory.create_file_data_source(
                path=local, file_type=self.file_type, encoding=self.encoding
            )
            yield from source.iter_batches(batch_size=batch_size, limit=limit)


def file_data_source(
    files: FilesConnConfig,
    relative: str,
    *,
    file_type: str | None = None,
    encoding: EncodingType = EncodingType.UTF_8,
) -> AbstractDataSource:
    """The data source reading the file at *relative* under the root of *files*."""
    if files.is_s3:
        return S3ObjectDataSource(
            s3=files.s3_config(),
            key=_s3_key(files, relative),
            file_type=file_type,
            encoding=encoding,
        )
    return DataSourceFactory.create_file_data_source(
        path=files.local_root / relative, file_type=file_type, encoding=encoding
    )


__all__ = [
    "FileEntry",
    "S3ObjectDataSource",
    "file_data_source",
    "list_files",
    "walk",
]
