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


def _table_rows(path: Path, **kwargs) -> list[dict]:
    chunker = TableChunker(batch_size=10, filename=path, **kwargs)
    return [row for batch in chunker for row in batch]


def test_table_chunker_splits_rows_on_the_separator(tmp_path: Path):
    path = tmp_path / "people.tsv"
    path.write_text("name\tcity\nAda, Countess\tLondon\nGrace\tNew York\n")

    assert _table_rows(path, sep="\t") == [
        {"name": "Ada, Countess", "city": "London"},
        {"name": "Grace", "city": "New York"},
    ]


def test_table_chunker_keeps_a_quoted_newline_in_one_record(tmp_path: Path):
    path = tmp_path / "notes.csv"
    path.write_text('id,note\n1,"first line\nsecond line"\n2,plain\n')

    assert _table_rows(path) == [
        {"id": "1", "note": "first line\nsecond line"},
        {"id": "2", "note": "plain"},
    ]


def test_table_chunker_reads_a_quoted_header(tmp_path: Path):
    path = tmp_path / "quoted.csv"
    path.write_text('"last, first",age\n"Lovelace, Ada",36\n')

    assert _table_rows(path) == [{"last, first": "Lovelace, Ada", "age": "36"}]


def test_table_chunker_header_has_no_carriage_return(tmp_path: Path):
    path = tmp_path / "crlf.csv"
    path.write_bytes(b"id,name\r\n1,Ada\r\n")

    assert _table_rows(path) == [{"id": "1", "name": "Ada"}]


def test_table_chunker_keeps_trailing_empty_fields(tmp_path: Path):
    path = tmp_path / "sparse.tsv"
    path.write_text("a\tb\tc\n1\t\t\n")

    assert _table_rows(path, sep="\t") == [{"a": "1", "b": "", "c": ""}]


def test_table_chunker_limit_counts_records(tmp_path: Path):
    path = tmp_path / "notes.csv"
    path.write_text('id,note\n1,"a\nb"\n2,c\n3,d\n')
    chunker = TableChunker(batch_size=10, limit=2, filename=path)

    rows = [row for batch in chunker for row in batch]

    assert [row["id"] for row in rows] == ["1", "2"]


def test_table_chunker_empty_file_yields_nothing(tmp_path: Path):
    path = tmp_path / "empty.csv"
    path.write_text("")

    assert _table_rows(path) == []
