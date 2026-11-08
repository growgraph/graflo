"""NebulaGraph writes: INSERT statements, space options, and the bulk writer's sequence."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

import pytest

from graflo.architecture.schema import Schema
from graflo.connections.bulk import BulkConfig
from graflo.connections.onto import NebulaConfig
from graflo.db.bulk import BulkEdgeChunk, BulkVertexChunk
from graflo.db.conn import SchemaExistsError
from graflo.db.nebula import conn as nebula_conn
from graflo.db.nebula.query import fold_rows, insert_edges_ngql, insert_vertices_ngql
from graflo.db.nebula.util import fitted_vid_length

MB = 1 << 20

# -- statements ---------------------------------------------------------------


def test_every_value_kind_is_written_as_the_upsert_writes_it() -> None:
    doc = {
        "none": None,
        "yes": True,
        "int": -7,
        "float": 1.5,
        "dec": Decimal("2.25"),
        "dt": datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
        "day": date(2024, 1, 2),
        "at": time(3, 4, 5),
        "list": ["a", 1],
        "map": {"k": 'v"'},
        "text": 'say "hi" \\ bye',
    }
    columns = [*doc, "absent"]
    [statement] = insert_vertices_ngql("T", columns, [('T::a"b', doc)], max_bytes=MB)
    assert statement == (
        "INSERT VERTEX `T` (`none`, `yes`, `int`, `float`, `dec`, `dt`, `day`, "
        "`at`, `list`, `map`, `text`, `absent`) VALUES "
        '"T::a\\"b":(NULL, true, -7, 1.5, 2.25, "2024-01-02T03:04:05+00:00", '
        '"2024-01-02", "03:04:05", "[\\"a\\", 1]", "{\\"k\\": \\"v\\\\\\"\\"}", '
        '"say \\"hi\\" \\\\ bye", NULL)'
    )


def test_an_edge_carries_its_rank_when_given_one() -> None:
    edges = [("a", "b", None, {"w": 1}), ("a", "b", 5, {"w": 2})]
    assert insert_edges_ngql("e", ["w"], edges, max_bytes=MB) == [
        'INSERT EDGE `e` (`w`) VALUES "a"->"b":(1), "a"->"b"@5:(2)'
    ]


def test_statements_split_on_row_boundaries_under_the_limit() -> None:
    rows = [(f"T::{i}", {"x": "y" * 10}) for i in range(10)]
    whole = insert_vertices_ngql("T", ["x"], rows, max_bytes=MB)
    assert len(whole) == 1
    one_row = len(insert_vertices_ngql("T", ["x"], rows[:1], max_bytes=MB)[0])
    limit = one_row + 2 * (one_row - len("INSERT VERTEX `T` (`x`) VALUES ")) + 4

    split = insert_vertices_ngql("T", ["x"], rows, max_bytes=limit)

    assert len(split) == 4 and all(len(s.encode()) <= limit for s in split)
    assert [s.count('"T::') for s in split] == [3, 3, 3, 1]
    values = [s.split(" VALUES ", 1)[1] for s in split]
    assert ", ".join(values) == whole[0].split(" VALUES ", 1)[1]


def test_the_split_counts_bytes_not_characters() -> None:
    rows = [(f"T::{i}", {"x": "é" * 100}) for i in range(4)]
    for statement in insert_vertices_ngql("T", ["x"], rows, max_bytes=300):
        assert len(statement.encode()) <= 300


def test_a_row_larger_than_the_limit_is_refused() -> None:
    with pytest.raises(ValueError, match="statement limit"):
        insert_vertices_ngql("T", ["x"], [("T::1", {"x": "y" * 100})], max_bytes=80)


def test_rows_fold_by_key_with_later_fields_winning() -> None:
    folded = fold_rows(
        [("a", {"x": 1, "y": 1}), ("b", {"x": 2}), ("a", {"y": None, "z": 3})]
    )
    assert folded == {"a": {"x": 1, "y": None, "z": 3}, "b": {"x": 2}}


# -- space options ------------------------------------------------------------


SCHEMA: dict[str, Any] = {
    "metadata": {"name": "g", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {"name": "Person", "properties": ["pid", "name"], "identity": ["pid"]},
                {
                    "name": "City",
                    "properties": [{"name": "cid", "type": "INT"}, "label"],
                    "identity": ["cid"],
                },
                {
                    "name": "Org",
                    "properties": [{"name": "oid", "type": "UUID"}],
                    "identity": ["oid"],
                },
            ]
        },
        "edge_config": {
            "edges": [
                {
                    "source": "Person",
                    "target": "City",
                    "relation": "in",
                    "properties": ["role"],
                },
                {
                    "source": "Org",
                    "target": "City",
                    "relation": "in",
                    "properties": ["since"],
                },
                {"source": "Person", "target": "Org", "relation": "works_at"},
            ]
        },
    },
}


def _schema() -> Schema:
    return Schema.model_validate(SCHEMA)


def test_the_vid_fits_the_longest_identity() -> None:
    # "Person" + "::" + a string; "City" + "::" + an int64; "Org" + "::" + a UUID.
    assert fitted_vid_length(_schema(), 128) == 136
    assert fitted_vid_length(_schema(), 16) == 48
    assert fitted_vid_length(None, 128) == 200


def test_unset_space_options_are_derived_and_set_ones_kept() -> None:
    config = NebulaConfig()
    assert config.max_statement_bytes == 4 * MB
    one = config.space_options(storage_hosts=1, vid_length=136)
    assert (one.vid_type, one.partition_num, one.replica_factor) == (
        "FIXED_STRING(136)",
        20,
        1,
    )
    three = config.space_options(storage_hosts=3, vid_length=136)
    assert (three.partition_num, three.replica_factor) == (60, 3)
    disks = NebulaConfig(storage_disks=8).space_options(storage_hosts=3, vid_length=8)
    assert disks.partition_num == 160

    explicit = NebulaConfig(
        vid_type="INT64", partition_num=7, replica_factor=1
    ).space_options(storage_hosts=5, vid_length=136)
    assert (explicit.vid_type, explicit.partition_num, explicit.replica_factor) == (
        "INT64",
        7,
        1,
    )


# -- the connection -----------------------------------------------------------


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def rows_as_dicts(self) -> list[dict[str, Any]]:
        return self._rows


class _Adapter:
    """Records statements and answers the ones the writer reads."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.tags: list[str] = []
        self.spaces: list[str] = []
        self.job_status: list[str] = []
        self._jobs = 0

    def use_space(self, space: str) -> None:
        self.statements.append(f"USE {space}")

    def execute(self, statement: str) -> _Result:
        self.statements.append(statement)
        if statement == "SHOW SPACES":
            return _Result([{"Name": s} for s in self.spaces])
        if statement == "SHOW TAGS":
            return _Result([{"Name": t} for t in self.tags])
        if statement == "SHOW HOSTS":
            return _Result([{"Status": "ONLINE"}])
        if statement == "SHOW PARTS":
            return _Result([{"Leader": "storaged:9779"}])
        if statement.startswith(("REBUILD", "SUBMIT JOB")):
            self._jobs += 1
            return _Result([{"New Job Id": self._jobs}])
        if statement.startswith("SHOW JOB "):
            status = self.job_status.pop(0) if self.job_status else "FINISHED"
            return _Result([{"Status": status}])
        if statement == "SHOW STATS":
            return _Result(
                [
                    {"Type": "Tag", "Name": "Person", "Count": 3},
                    {"Type": "Tag", "Name": "City", "Count": 2},
                    {"Type": "Edge", "Name": "in", "Count": 9},
                    {"Type": "Edge", "Name": "works_at", "Count": 4},
                    {"Type": "Space", "Name": "vertices", "Count": 5},
                ]
            )
        if statement.startswith("MATCH"):
            return _Result([{"n": 6 if "`Person`" in statement else 3}])
        if statement.startswith("DESCRIBE TAG"):
            return _Result([{"Field": "pid"}, {"Field": "name"}])
        if statement.startswith("DESCRIBE EDGE"):
            return _Result([{"Field": "role"}])
        return _Result([])


@pytest.fixture
def adapter(monkeypatch) -> _Adapter:
    fake = _Adapter()
    monkeypatch.setattr(nebula_conn, "create_adapter", lambda config: fake)
    monkeypatch.setattr(nebula_conn.time, "sleep", lambda s: None)
    return fake


def _connect() -> nebula_conn.NebulaConnection:
    return nebula_conn.NebulaConnection(
        NebulaConfig(uri="nebula://localhost:9669", schema_name="g")
    )


def _after(statements: list[str], start: int) -> list[str]:
    return [s for s in statements[start:] if not s.startswith("FETCH PROP")]


def test_bulk_begin_creates_a_sized_space_and_types_but_no_index(adapter) -> None:
    db = _connect()
    db.bulk_begin(_schema(), BulkConfig())

    create = next(s for s in adapter.statements if s.startswith("CREATE SPACE"))
    assert create == (
        "CREATE SPACE IF NOT EXISTS `g` "
        "(vid_type=FIXED_STRING(136), partition_num=20, replica_factor=1)"
    )
    assert any(
        s.startswith("CREATE TAG IF NOT EXISTS `Person`") for s in adapter.statements
    )
    assert "CREATE EDGE IF NOT EXISTS `in` (`role` string, `since` string)" in (
        adapter.statements
    )
    assert not any("INDEX" in s for s in adapter.statements)


def test_bulk_begin_refuses_a_space_holding_a_schema(adapter) -> None:
    adapter.spaces, adapter.tags = ["g"], ["Person"]
    db = _connect()
    with pytest.raises(SchemaExistsError, match="already exists"):
        db.bulk_begin(_schema(), BulkConfig())

    db.bulk_begin(_schema(), BulkConfig(), resume=True)
    assert not any(s.startswith(("DROP", "CREATE SPACE")) for s in adapter.statements)


def test_bulk_begin_drops_the_space_when_recreating(adapter) -> None:
    adapter.spaces, adapter.tags = ["g"], ["Person"]
    db = _connect()
    db.bulk_begin(_schema(), BulkConfig(), recreate=True)
    drop = adapter.statements.index("DROP SPACE IF EXISTS `g`")
    create = next(
        i for i, s in enumerate(adapter.statements) if s.startswith("CREATE SPACE")
    )
    assert drop < create


def test_chunks_are_inserted_with_every_column(adapter) -> None:
    db = _connect()
    start = len(adapter.statements)
    db.bulk_write_vertices(
        _schema(),
        BulkVertexChunk(
            chunk_id="v:0",
            class_name="Person",
            match_keys=["pid"],
            rows=[{"pid": "p1", "name": "Ann"}, {"pid": "p2"}],
            replay=True,
        ),
    )
    db.bulk_prepare_edges(_schema())
    db.bulk_write_edges(
        _schema(),
        BulkEdgeChunk(
            chunk_id="e:0",
            source_class="Person",
            target_class="City",
            relation_name="in",
            match_keys_source=["pid"],
            match_keys_target=["cid"],
            collection_name=None,
            rows=[({"pid": "p1"}, {"cid": 1}, {"role": "home"})],
        ),
    )
    assert _after(adapter.statements, start) == [
        (
            "INSERT VERTEX `Person` (`pid`, `name`) VALUES "
            '"Person::p1":("p1", "Ann"), "Person::p2":("p2", NULL)'
        ),
        (
            "INSERT EDGE `in` (`role`, `since`) VALUES "
            '"Person::p1"->"City::1":("home", NULL)'
        ),
    ]


def test_bulk_finish_builds_indexes_after_the_load_then_counts(adapter) -> None:
    db = _connect()
    start = len(adapter.statements)
    counts = db.bulk_finish(_schema())

    assert _after(adapter.statements, start) == [
        "CREATE TAG INDEX IF NOT EXISTS `idx_City_cid` ON `City` (`cid`)",
        "CREATE TAG INDEX IF NOT EXISTS `idx_Org_oid` ON `Org` (`oid`(256))",
        "CREATE TAG INDEX IF NOT EXISTS `idx_Person_pid` ON `Person` (`pid`(256))",
        "REBUILD TAG INDEX `idx_City_cid`, `idx_Org_oid`, `idx_Person_pid`",
        "SHOW JOB 1",
        "SUBMIT JOB STATS",
        "SHOW JOB 2",
        "SHOW STATS",
        "MATCH (:`Person`)-[e:`in`]->(:`City`) RETURN count(e) AS n",
        "MATCH (:`Org`)-[e:`in`]->(:`City`) RETURN count(e) AS n",
    ]
    assert counts.vertices == {"Person": 3, "City": 2, "Org": 0}
    assert counts.edges == {
        ("Person", "in", "City"): 6,
        ("Org", "in", "City"): 3,
        ("Person", "works_at", "Org"): 4,
    }


def test_a_failed_job_is_submitted_again(adapter) -> None:
    adapter.job_status = ["RUNNING", "FAILED", "FINISHED"]
    db = _connect()
    db._run_job("SUBMIT JOB STATS")
    jobs = [s for s in adapter.statements if "JOB" in s]
    assert jobs == [
        "SUBMIT JOB STATS",
        "SHOW JOB 1",
        "SHOW JOB 1",
        "SUBMIT JOB STATS",
        "SHOW JOB 2",
    ]


def test_a_job_that_keeps_failing_is_raised(adapter) -> None:
    adapter.job_status = ["FAILED"] * 10
    with pytest.raises(RuntimeError, match="ended FAILED"):
        _connect()._run_job("SUBMIT JOB STATS")


# -- the upsert path ------------------------------------------------------------


def test_complete_vertices_are_inserted_and_partial_ones_upserted(adapter) -> None:
    db = _connect()
    start = len(adapter.statements)
    db.upsert_docs_batch(
        [
            {"pid": "p1", "name": "Ann"},
            {"pid": "p2"},
            {"pid": "p1", "name": "Ann B."},
        ],
        "Person",
        ["pid"],
    )
    assert adapter.statements[start:] == [
        "DESCRIBE TAG `Person`",
        'INSERT VERTEX `Person` (`pid`, `name`) VALUES "Person::p1":("p1", "Ann B.")',
        'UPSERT VERTEX ON `Person` "Person::p2" SET `pid` = "p2"',
    ]


def test_edges_are_folded_then_inserted_upserted_or_created_bare(adapter) -> None:
    db = _connect()
    start = len(adapter.statements)
    db.insert_edges_batch(
        [
            [{"pid": "p1"}, {"cid": 1}, {"role": "home", "x": 1}],
            [{"pid": "p2"}, {"cid": 1}, {"x": 1}],
            [{"pid": "p3"}, {"cid": 1}, {}],
            [{"pid": "p1"}, {"cid": 1}, {"role": "work"}],
        ],
        "Person",
        "City",
        "in",
        ("pid",),
        ("cid",),
    )
    assert adapter.statements[start:] == [
        "DESCRIBE EDGE `in`",
        'INSERT EDGE `in` (`role`) VALUES "Person::p1"->"City::1":("work")',
        (
            "INSERT EDGE IF NOT EXISTS `in` () VALUES "
            '"Person::p2"->"City::1":(), "Person::p3"->"City::1":()'
        ),
    ]


def test_an_edge_identity_beyond_its_endpoints_ranks_the_edge() -> None:
    from graflo.db.nebula.util import edge_rank

    assert edge_rank({"role": "home", "since": 1}, []) is None
    home = edge_rank({"role": "home", "since": 1}, ["role"])
    assert home == edge_rank({"role": "home", "since": 2}, ["role"])
    assert home != edge_rank({"role": "work", "since": 1}, ["role"])
    assert home is not None and -(2**63) <= home < 2**63
