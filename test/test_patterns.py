import pathlib
import pytest

from graflo.util.onto import Patterns, FilePattern, TablePattern


def test_patterns():
    # Create Patterns with FilePattern instances
    patterns = Patterns()

    # Add file patterns directly
    pattern_a = FilePattern(
        regex=".*", sub_path=pathlib.Path("dir_a/dir_b"), resource_name="a"
    )
    pattern_b = FilePattern(
        regex="^asd", sub_path=pathlib.Path("./"), resource_name="b"
    )

    patterns.add_file_pattern("a", pattern_a)
    patterns.add_file_pattern("b", pattern_b)

    # Test that patterns work correctly
    assert patterns.patterns["a"].sub_path is not None
    assert isinstance(patterns.patterns["a"].sub_path / "a", pathlib.Path)
    assert patterns.patterns["b"].sub_path is not None
    assert str(patterns.patterns["b"].sub_path / "a") == "a"

    # Test that patterns can be accessed by name
    assert "a" in patterns.patterns
    assert "b" in patterns.patterns
    assert patterns.get_resource_type("a") == "file"
    assert patterns.get_resource_type("b") == "file"


def test_file_pattern_limit_rows():
    """Test FilePattern with limit_rows parameter."""
    pattern = FilePattern(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        limit_rows=100,
    )
    assert pattern.limit_rows == 100
    assert pattern.date_field is None
    assert pattern.date_filter is None


def test_file_pattern_date_validation():
    """Test FilePattern date filtering parameter validation."""
    # Should raise error if date_filter is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        FilePattern(
            regex=r".*\.csv$",
            sub_path=pathlib.Path("./data"),
            date_filter="> '2020-10-10'",
        )

    # Should raise error if date_range_start is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        FilePattern(
            regex=r".*\.csv$",
            sub_path=pathlib.Path("./data"),
            date_range_start="2015-11-11",
        )

    # Should raise error if date_range_days is set without date_range_start
    with pytest.raises(ValueError, match="date_range_start is required"):
        FilePattern(
            regex=r".*\.csv$",
            sub_path=pathlib.Path("./data"),
            date_field="dt",
            date_range_days=30,
        )

    # Should work with all required parameters
    pattern = FilePattern(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        date_field="dt",
        date_filter="> '2020-10-10'",
    )
    assert pattern.date_field == "dt"
    assert pattern.date_filter == "> '2020-10-10'"


def test_table_pattern_limit_rows():
    """Test TablePattern with limit_rows parameter."""
    pattern = TablePattern(
        table_name="events",
        schema_name="public",
        limit_rows=1000,
    )
    assert pattern.limit_rows == 1000
    assert pattern.table_name == "events"
    assert pattern.schema_name == "public"


def test_table_pattern_date_validation():
    """Test TablePattern date filtering parameter validation."""
    # Should raise error if date_filter is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        TablePattern(
            table_name="events",
            date_filter="> '2020-10-10'",
        )

    # Should raise error if date_range_start is set without date_field
    with pytest.raises(ValueError, match="date_field is required"):
        TablePattern(
            table_name="events",
            date_range_start="2015-11-11",
        )

    # Should raise error if date_range_days is set without date_range_start
    with pytest.raises(ValueError, match="date_range_start is required"):
        TablePattern(
            table_name="events",
            date_field="dt",
            date_range_days=30,
        )

    # Should work with all required parameters
    pattern = TablePattern(
        table_name="events",
        date_field="dt",
        date_filter="> '2020-10-10'",
    )
    assert pattern.date_field == "dt"
    assert pattern.date_filter == "> '2020-10-10'"


def test_table_pattern_build_where_clause_no_filter():
    """Test build_where_clause with no filters."""
    pattern = TablePattern(table_name="events")
    assert pattern.build_where_clause() == ""


def test_table_pattern_build_where_clause_date_filter():
    """Test build_where_clause with date_filter."""
    # Test with quoted date
    pattern = TablePattern(
        table_name="events",
        date_field="created_at",
        date_filter="> '2020-10-10'",
    )
    where_clause = pattern.build_where_clause()
    assert '"created_at"' in where_clause
    assert "> '2020-10-10'" in where_clause

    # Test with unquoted date (should add quotes)
    pattern2 = TablePattern(
        table_name="events",
        date_field="dt",
        date_filter="> 2020-10-10",
    )
    where_clause2 = pattern2.build_where_clause()
    assert '"dt"' in where_clause2
    assert "> '2020-10-10'" in where_clause2

    # Test with >= operator
    pattern3 = TablePattern(
        table_name="events",
        date_field="dt",
        date_filter=">= '2015-11-11'",
    )
    where_clause3 = pattern3.build_where_clause()
    assert '"dt"' in where_clause3
    assert ">= '2015-11-11'" in where_clause3


def test_table_pattern_build_where_clause_date_range():
    """Test build_where_clause with date_range_start and date_range_days."""
    pattern = TablePattern(
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


def test_table_pattern_build_where_clause_complex():
    """Test build_where_clause with various date filter formats."""
    # Test with different operators
    pattern1 = TablePattern(
        table_name="events",
        date_field="timestamp",
        date_filter="< '2023-01-01'",
    )
    where1 = pattern1.build_where_clause()
    assert '"timestamp"' in where1
    assert "< '2023-01-01'" in where1

    # Test with != operator
    pattern2 = TablePattern(
        table_name="events",
        date_field="dt",
        date_filter="!= '2020-01-01'",
    )
    where2 = pattern2.build_where_clause()
    assert '"dt"' in where2
    assert "!= '2020-01-01'" in where2


def test_patterns_with_filtering():
    """Test Patterns collection with filtering parameters."""
    patterns = Patterns()

    # Add file pattern with limit
    file_pattern = FilePattern(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        limit_rows=500,
        resource_name="users",
    )
    patterns.add_file_pattern("users", file_pattern)

    # Add table pattern with date filter
    table_pattern = TablePattern(
        table_name="events",
        schema_name="public",
        date_field="created_at",
        date_filter="> '2020-10-10'",
        limit_rows=1000,
        resource_name="events",
    )
    patterns.add_table_pattern("events", table_pattern)

    # Verify patterns are stored correctly
    assert patterns.patterns["users"].limit_rows == 500
    assert patterns.patterns["events"].limit_rows == 1000
    assert patterns.patterns["events"].date_field == "created_at"
    assert patterns.patterns["events"].date_filter == "> '2020-10-10'"


def test_table_pattern_sql_query_building():
    """Test that TablePattern builds correct SQL queries with filters."""
    # Test the query building logic directly (as used in caster.py)
    table_pattern = TablePattern(
        table_name="events",
        schema_name="public",
        date_field="dt",
        date_filter="> '2020-10-10'",
        limit_rows=100,
        resource_name="events",
    )

    # Test WHERE clause building
    where_clause = table_pattern.build_where_clause()
    expected_where = "\"dt\" > '2020-10-10'"
    assert where_clause == expected_where

    # Test that the query would include WHERE and LIMIT (as built in caster.py)
    base_query = 'SELECT * FROM "public"."events"'
    if where_clause:
        full_query = f"{base_query} WHERE {where_clause}"
    else:
        full_query = base_query
    if table_pattern.limit_rows:
        full_query += f" LIMIT {table_pattern.limit_rows}"

    assert "WHERE" in full_query
    assert "LIMIT 100" in full_query
    assert "> '2020-10-10'" in full_query
    assert '"dt"' in full_query

    # Test with only limit (no date filter)
    pattern_no_date = TablePattern(
        table_name="users",
        schema_name="public",
        limit_rows=50,
    )
    where_clause_no_date = pattern_no_date.build_where_clause()
    assert where_clause_no_date == ""

    query_no_date = 'SELECT * FROM "public"."users"'
    if pattern_no_date.limit_rows:
        query_no_date += f" LIMIT {pattern_no_date.limit_rows}"

    assert "LIMIT 50" in query_no_date
    assert "WHERE" not in query_no_date


def test_table_pattern_date_range_sql():
    """Test SQL query building with date range."""
    pattern = TablePattern(
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
