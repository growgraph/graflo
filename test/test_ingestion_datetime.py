"""Tests for ingestion datetime range params and SQL WHERE building."""

from graflo.hq.caster import Caster, IngestionParams
from graflo.util.onto import TablePattern


def test_ingestion_params_datetime_defaults():
    """IngestionParams has None for datetime fields by default."""
    params = IngestionParams()
    assert params.datetime_after is None
    assert params.datetime_before is None
    assert params.datetime_column is None


def test_ingestion_params_datetime_set():
    """IngestionParams accepts datetime_after, datetime_before, datetime_column."""
    params = IngestionParams(
        datetime_after="2020-01-01T00:00:00",
        datetime_before="2020-12-31T23:59:59",
        datetime_column="created_at",
    )
    assert params.datetime_after == "2020-01-01T00:00:00"
    assert params.datetime_before == "2020-12-31T23:59:59"
    assert params.datetime_column == "created_at"


def test_datetime_range_where_sql_empty():
    """_datetime_range_where_sql returns empty when both bounds None."""
    out = Caster._datetime_range_where_sql(None, None, "dt")
    assert out == ""


def test_datetime_range_where_sql_both_bounds():
    """_datetime_range_where_sql produces [after, before) with AND."""
    out = Caster._datetime_range_where_sql(
        "2020-01-01",
        "2020-12-31",
        "created_at",
    )
    assert "\"created_at\" >= '2020-01-01'" in out
    assert "\"created_at\" < '2020-12-31'" in out
    assert " AND " in out


def test_datetime_range_where_sql_only_after():
    """_datetime_range_where_sql with only datetime_after."""
    out = Caster._datetime_range_where_sql("2020-06-01", None, "dt")
    assert out == "\"dt\" >= '2020-06-01'"


def test_datetime_range_where_sql_only_before():
    """_datetime_range_where_sql with only datetime_before."""
    out = Caster._datetime_range_where_sql(None, "2021-01-01", "ts")
    assert out == "\"ts\" < '2021-01-01'"


def test_datetime_range_where_sql_iso_format():
    """_datetime_range_where_sql accepts ISO datetime strings."""
    out = Caster._datetime_range_where_sql(
        "2020-01-15T10:30:00",
        "2020-01-15T18:00:00",
        "updated_at",
    )
    assert "2020-01-15T10:30:00" in out
    assert "2020-01-15T18:00:00" in out
    assert "updated_at" in out


def test_sql_query_where_combines_pattern_and_ingestion_datetime():
    """Query WHERE combines TablePattern date_filter and ingestion datetime range."""
    # Simulate the logic in _register_sql_table_sources: pattern WHERE + datetime WHERE
    pattern = TablePattern(
        table_name="events",
        date_field="dt",
        date_filter="!= '2020-01-01'",
    )
    pattern_where = pattern.build_where_clause()
    datetime_where = Caster._datetime_range_where_sql(
        "2020-06-01",
        "2020-07-01",
        pattern.date_field or "dt",
    )
    where_parts = [p for p in [pattern_where, datetime_where] if p]
    combined = " AND ".join(where_parts)
    assert "\"dt\" != '2020-01-01'" in combined
    assert "\"dt\" >= '2020-06-01'" in combined
    assert "\"dt\" < '2020-07-01'" in combined
    assert combined.count(" AND ") == 2
