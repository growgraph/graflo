"""Real tests for ingesting in a date range from PostgreSQL.

Requires PostgreSQL running (e.g. docker/postgres). Uses mock_schema tables
and asserts that datetime_after/datetime_before and per-resource date_field
filter rows correctly.
"""

from graflo.filter.sql import datetime_range_where_sql
from graflo.hq.caster import IngestionParams
from graflo.hq.graph_engine import GraphEngine
from graflo.onto import DBType
from graflo.util.onto import TablePattern


def _set_purchase_dates(postgres_conn):
    """Set purchases to known dates so we can test range [2020-02-01, 2020-06-01)."""
    updates = [
        (1, "2020-01-10"),
        (2, "2020-03-15"),
        (3, "2020-05-20"),
        (4, "2020-07-01"),
        (5, "2020-09-01"),
        (6, "2020-12-01"),
    ]
    with postgres_conn.conn.cursor() as cursor:
        for pid, dt in updates:
            cursor.execute(
                "UPDATE purchases SET purchase_date = %s::timestamp WHERE id = %s",
                (dt, pid),
            )
        postgres_conn.conn.commit()


def test_datetime_columns_sets_date_field_on_patterns(conn_conf, load_mock_schema):
    """create_patterns(..., datetime_columns=...) sets date_field on TablePatterns."""
    _ = load_mock_schema  # ensure tables exist
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    patterns = engine.create_patterns(
        conn_conf,
        schema_name="public",
        datetime_columns={
            "purchases": "purchase_date",
            "users": "created_at",
        },
    )
    assert patterns.table_patterns["purchases"].date_field == "purchase_date"
    assert patterns.table_patterns["users"].date_field == "created_at"
    # Tables not in the map have no date_field
    if "follows" in patterns.table_patterns:
        assert patterns.table_patterns["follows"].date_field is None


def test_ingest_datetime_range_postgres(postgres_conn, load_mock_schema):
    """Real Postgres: query with datetime_after/datetime_before returns only rows in range."""
    _ = load_mock_schema
    _set_purchase_dates(postgres_conn)

    pattern = TablePattern(
        table_name="purchases",
        schema_name="public",
        resource_name="purchases",
        date_field="purchase_date",
    )
    datetime_where = datetime_range_where_sql(
        "2020-02-01",
        "2020-06-01",
        pattern.date_field or "purchase_date",
    )
    assert datetime_where
    query = f'SELECT * FROM "public"."purchases" WHERE {datetime_where}'

    rows = postgres_conn.read(query)
    # Range [2020-02-01, 2020-06-01): only id 2 (2020-03-15) and id 3 (2020-05-20)
    assert len(rows) == 2
    ids = {r["id"] for r in rows}
    assert ids == {2, 3}


def test_ingest_datetime_range_with_global_column(postgres_conn, load_mock_schema):
    """IngestionParams.datetime_column is used when pattern has no date_field."""
    _ = load_mock_schema
    _set_purchase_dates(postgres_conn)

    pattern = TablePattern(
        table_name="purchases",
        schema_name="public",
        resource_name="purchases",
        date_field=None,
    )
    ingestion_params = IngestionParams(
        datetime_after="2020-02-01",
        datetime_before="2020-06-01",
        datetime_column="purchase_date",
    )
    date_column = pattern.date_field or ingestion_params.datetime_column
    assert date_column == "purchase_date"
    datetime_where = datetime_range_where_sql(
        ingestion_params.datetime_after,
        ingestion_params.datetime_before,
        date_column,
    )
    query = f'SELECT * FROM "public"."purchases" WHERE {datetime_where}'
    rows = postgres_conn.read(query)
    assert len(rows) == 2
    assert {r["id"] for r in rows} == {2, 3}
