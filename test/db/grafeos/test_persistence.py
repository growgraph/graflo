"""A Grafeo file database: shared by connections, durable across a reopen."""

from __future__ import annotations

import datetime as dt

from graflo.connections.grafeo import GrafeoConfig
from graflo.db.grafeo import GrafeoConnection, close_database


def test_a_file_database_survives_close_and_reopen(tmp_path) -> None:
    path = tmp_path / "kg.grafeo"
    conf = GrafeoConfig(path=path, database="kg")
    GrafeoConnection(conf).upsert_docs_batch([{"id": 1, "name": "a"}], "P", ["id"])
    close_database(path)

    reopened = GrafeoConnection(GrafeoConfig(path=path, database="kg"))
    try:
        assert reopened.fetch_docs("P") == [{"id": 1, "name": "a"}]
    finally:
        close_database(path)


def test_two_connections_to_one_file_share_the_database(tmp_path) -> None:
    path = tmp_path / "kg.grafeo"
    writer = GrafeoConnection(GrafeoConfig(path=path, database="kg"))
    reader = GrafeoConnection(GrafeoConfig(path=path, database="kg"))
    try:
        writer.upsert_docs_batch([{"id": 1}], "P", ["id"])
        assert reader.fetch_docs("P") == [{"id": 1}]
    finally:
        close_database(path)


def test_graphs_of_one_database_are_separate(tmp_path) -> None:
    path = tmp_path / "kg.grafeo"
    one = GrafeoConnection(GrafeoConfig(path=path, database="one"))
    two = GrafeoConnection(GrafeoConfig(path=path, database="two"))
    try:
        one.upsert_docs_batch([{"id": 1}], "P", ["id"])
        assert two.fetch_docs("P") == []
    finally:
        close_database(path)


def test_a_datetime_is_read_back_as_a_utc_instant(conn_conf, test_graph_name) -> None:
    conn = GrafeoConnection(conn_conf)
    paris = dt.timezone(dt.timedelta(hours=1))
    written = dt.datetime(2026, 1, 2, 13, 0, tzinfo=paris)
    conn.upsert_docs_batch(
        [{"id": 1, "at": written}, {"id": 2, "at": dt.datetime(2026, 1, 2, 12, 0)}],
        "E",
        ["id"],
    )
    by_id = {doc["id"]: doc["at"] for doc in conn.fetch_docs("E")}
    assert by_id[1] == written
    assert by_id[1].tzinfo == dt.UTC
    assert by_id[2] == dt.datetime(2026, 1, 2, 12, 0, tzinfo=dt.UTC)


def test_clearing_one_graph_keeps_another_graph_s_indexes(tmp_path) -> None:
    path = tmp_path / "kg.grafeo"
    one = GrafeoConnection(GrafeoConfig(path=path, database="one"))
    two = GrafeoConnection(GrafeoConfig(path=path, database="two"))
    try:
        for conn in (one, two):
            conn.execute("CREATE INDEX `P_id` IF NOT EXISTS FOR (x:`P`) ON (x.`id`)")
        one.delete_graph_structure(delete_all=True)
        assert one.execute("SHOW INDEXES").rows == []
        assert [row["name"] for row in two.execute("SHOW INDEXES").rows] == ["P_id"]
    finally:
        close_database(path)
