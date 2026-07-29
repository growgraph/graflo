import os
from pathlib import Path

from graflo.data_source.chunker import (
    FileChunker,
    JsonChunker,
    JsonlChunker,
    TableChunker,
    TrivialChunker,
)


def test_trivial():
    array = [{"a": v} for v in range(9)]
    ch = TrivialChunker(batch_size=5, array=array)
    for _ in ch:
        pass
    assert ch.cnt == 9


def test_file_chunker():
    filename = Path(
        os.path.join(os.path.dirname(__file__), "../data/ticker/ticker.csv.gz")
    )
    ch = FileChunker(batch_size=5, limit=6, filename=filename)
    for _ in ch:
        pass
    assert ch.cnt == 6
    ch = FileChunker(batch_size=90, limit=250, filename=filename)
    for _ in ch:
        pass
    assert ch.cnt == 200


def test_table_chunker():
    filename = Path(
        os.path.join(os.path.dirname(__file__), "../data/ticker/ticker.csv.gz")
    )
    ch = TableChunker(batch_size=5, limit=6, filename=filename)
    for item in ch:
        assert set(item[0].keys()) == set(ch.header)
    assert ch.cnt == 6


def test_jsonl_chunker():
    filename = Path(
        os.path.join(os.path.dirname(__file__), "../data/jsonl/e00000000.jsonl.gz")
    )
    ch = JsonlChunker(batch_size=5, limit=6, filename=filename)
    for item in ch:
        assert isinstance(item[0], dict)


def test_json_chunker():
    filename = Path(os.path.join(os.path.dirname(__file__), "../data/wos/wos.json.gz"))
    ch = JsonChunker(batch_size=5, limit=6, filename=filename)
    for item in ch:
        assert isinstance(item[0], dict)
    assert ch.cnt == 6
