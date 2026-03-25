import pathlib
import pytest

from graflo.architecture.contract.bindings import (
    Bindings,
    FileConnector,
    TableConnector,
)


def test_connectors():
    # Create Bindings with FileConnector instances
    bindings = Bindings()

    # Add file connectors directly
    connector_a = FileConnector(
        regex=".*", sub_path=pathlib.Path("dir_a/dir_b"), resource_name="a"
    )
    connector_b = FileConnector(
        regex="^asd", sub_path=pathlib.Path("./"), resource_name="b"
    )

    bindings.add_file_connector("a", connector_a)
    bindings.add_file_connector("b", connector_b)

    # Test that connectors work correctly (narrow to FileConnector for .sub_path)
    connector_a_loaded = bindings.connectors["a"]
    connector_b_loaded = bindings.connectors["b"]
    assert isinstance(connector_a_loaded, FileConnector)
    assert isinstance(connector_b_loaded, FileConnector)
    assert connector_a_loaded.sub_path is not None
    assert isinstance(connector_a_loaded.sub_path / "a", pathlib.Path)
    assert connector_b_loaded.sub_path is not None
    assert str(connector_b_loaded.sub_path / "a") == "a"

    # Test that connectors can be accessed by name
    assert "a" in bindings.connectors
    assert "b" in bindings.connectors
    assert bindings.get_resource_type("a") == "file"
    assert bindings.get_resource_type("b") == "file"


def test_file_connector_basic():
    """Test FileConnector basic functionality."""
    pattern = FileConnector(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
    )
    assert pattern.regex == r".*\.csv$"
    assert pattern.date_field is None
    assert pattern.date_filter is None


def test_file_connector_date_validation():
    """Test FileConnector date filtering parameter validation."""
    # Should raise error if date_filter is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        FileConnector(
            regex=r".*\.csv$",
            sub_path=pathlib.Path("./data"),
            date_filter="> '2020-10-10'",
        )

    # Should raise error if date_range_start is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        FileConnector(
            regex=r".*\.csv$",
            sub_path=pathlib.Path("./data"),
            date_range_start="2015-11-11",
        )

    # Should raise error if date_range_days is set without date_range_start
    with pytest.raises(ValueError, match="date_range_start is required"):
        FileConnector(
            regex=r".*\.csv$",
            sub_path=pathlib.Path("./data"),
            date_field="dt",
            date_range_days=30,
        )

    # Should work with all required parameters
    pattern = FileConnector(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        date_field="dt",
        date_filter="> '2020-10-10'",
    )
    assert pattern.date_field == "dt"
    assert pattern.date_filter == "> '2020-10-10'"


def test_table_connector_basic():
    """Test TableConnector basic functionality."""
    pattern = TableConnector(
        table_name="events",
        schema_name="public",
    )
    assert pattern.table_name == "events"
    assert pattern.schema_name == "public"


def test_table_connector_date_validation():
    """Test TableConnector date filtering parameter validation."""
    # Should raise error if date_filter is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        TableConnector(
            table_name="events",
            date_filter="> '2020-10-10'",
        )

    # Should raise error if date_range_start is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        TableConnector(
            table_name="events",
            date_range_start="2015-11-11",
        )

    # Should raise error if date_range_days is set without date_range_start
    with pytest.raises(ValueError, match="date_range_start is required"):
        TableConnector(
            table_name="events",
            date_field="dt",
            date_range_days=30,
        )

    # Should work with all required parameters
    pattern = TableConnector(
        table_name="events",
        date_field="dt",
        date_filter="> '2020-10-10'",
    )
    assert pattern.date_field == "dt"
    assert pattern.date_filter == "> '2020-10-10'"


def test_table_connector_build_where_clause_no_filter():
    """Test build_where_clause with no filters."""
    pattern = TableConnector(table_name="events")
    assert pattern.build_where_clause() == ""


def test_table_connector_build_where_clause_date_filter():
    """Test build_where_clause with date_filter."""
    # Test with quoted date
    pattern = TableConnector(
        table_name="events",
        date_field="created_at",
        date_filter="> '2020-10-10'",
    )
    where_clause = pattern.build_where_clause()
    assert '"created_at"' in where_clause
    assert "> '2020-10-10'" in where_clause

    # Test with unquoted date (should add quotes)
    pattern2 = TableConnector(
        table_name="events",
        date_field="dt",
        date_filter="> 2020-10-10",
    )
    where_clause2 = pattern2.build_where_clause()
    assert '"dt"' in where_clause2
    assert "> '2020-10-10'" in where_clause2

    # Test with >= operator
    pattern3 = TableConnector(
        table_name="events",
        date_field="dt",
        date_filter=">= '2015-11-11'",
    )
    where_clause3 = pattern3.build_where_clause()
    assert '"dt"' in where_clause3
    assert ">= '2015-11-11'" in where_clause3


def test_table_connector_build_where_clause_date_range():
    """Test build_where_clause with date_range_start and date_range_days."""
    pattern = TableConnector(
        table_name="transactions",
        date_field="dt",
        date_range_start="2015-11-11",
        date_range_days=30,
    )
    where_clause = pattern.build_where_clause()
    # Should contain both conditions
    assert '"dt"' in where_clause
    assert ">= '2015-11-11'" in where_clause
    assert "INTERVAL '30 days'" in where_clause
    assert "AND" in where_clause

    # Verify the range logic: dt >= start AND dt < start + interval
    assert where_clause.count(">=") == 1
    assert where_clause.count("<") == 1


def test_table_connector_build_where_clause_complex():
    """Test build_where_clause with various date filter formats."""
    # Test with different operators
    connector = TableConnector(
        table_name="events",
        date_field="timestamp",
        date_filter="< '2023-01-01'",
    )
    where1 = connector.build_where_clause()
    assert '"timestamp"' in where1
    assert "< '2023-01-01'" in where1

    # Test with != operator
    connector2 = TableConnector(
        table_name="events",
        date_field="dt",
        date_filter="!= '2020-01-01'",
    )
    where2 = connector2.build_where_clause()
    assert '"dt"' in where2
    assert "!= '2020-01-01'" in where2


def test_connectors_with_filtering():
    """Test Bindings collection with filtering parameters."""
    bindings = Bindings()

    # Add file pattern
    file_connector = FileConnector(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        resource_name="users",
    )
    bindings.add_file_connector("users", file_connector)

    # Add table pattern with date filter
    table_connector = TableConnector(
        table_name="events",
        schema_name="public",
        date_field="created_at",
        date_filter="> '2020-10-10'",
        resource_name="events",
    )
    bindings.add_table_connector("events", table_connector)

    # Verify connectors are stored correctly (narrow with isinstance checks)
    users_pattern = bindings.connectors["users"]
    events_pattern = bindings.connectors["events"]
    assert isinstance(users_pattern, FileConnector)
    assert users_pattern.regex == r".*\.csv$"
    assert isinstance(events_pattern, TableConnector)
    assert events_pattern.date_field == "created_at"
    assert events_pattern.date_filter == "> '2020-10-10'"


def test_table_connector_sql_query_building():
    """Test that TableConnector builds correct SQL queries with filters."""
    # Test the query building logic directly (as used in caster.py)
    table_connector = TableConnector(
        table_name="events",
        schema_name="public",
        date_field="dt",
        date_filter="> '2020-10-10'",
        resource_name="events",
    )

    # Test WHERE clause building
    where_clause = table_connector.build_where_clause()
    expected_where = "\"dt\" > '2020-10-10'"
    assert where_clause == expected_where

    base_query = 'SELECT * FROM "public"."events"'
    if where_clause:
        full_query = f"{base_query} WHERE {where_clause}"
    else:
        full_query = base_query

    assert "WHERE" in full_query
    assert "> '2020-10-10'" in full_query
    assert '"dt"' in full_query

    # Test with no date filter
    pattern_no_date = TableConnector(
        table_name="users",
        schema_name="public",
    )
    where_clause_no_date = pattern_no_date.build_where_clause()
    assert where_clause_no_date == ""

    query_no_date = 'SELECT * FROM "public"."users"'
    assert "WHERE" not in query_no_date


def test_table_connector_date_range_sql():
    """Test SQL query building with date range."""
    pattern = TableConnector(
        table_name="transactions",
        date_field="dt",
        date_range_start="2015-11-11",
        date_range_days=30,
    )

    where_clause = pattern.build_where_clause()
    # Should have both conditions joined with AND
    assert "AND" in where_clause
    assert ">=" in where_clause
    assert "<" in where_clause
    assert "2015-11-11" in where_clause
    assert "INTERVAL '30 days'" in where_clause

    # Verify the full query structure
    base_query = 'SELECT * FROM "public"."transactions"'
    full_query = f"{base_query} WHERE {where_clause}"
    assert "WHERE" in full_query
    assert where_clause.count("AND") == 1  # Should have exactly one AND
