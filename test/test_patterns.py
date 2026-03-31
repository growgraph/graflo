import pathlib
import pytest

from graflo.architecture.contract.bindings import (
    Bindings,
    BoundSourceKind,
    FileConnector,
    ResourceConnectorBinding,
    TableConnector,
)


def test_connectors():
    # Create Bindings with FileConnector instances
    bindings = Bindings()

    # Add file connectors directly
    connector_a = FileConnector(regex=".*", sub_path=pathlib.Path("dir_a/dir_b"))
    connector_b = FileConnector(regex="^asd", sub_path=pathlib.Path("./"))

    bindings.add_connector(connector_a)
    bindings.add_connector(connector_b)
    bindings.bind_resource("a", connector_a)
    bindings.bind_resource("b", connector_b)

    # Test that connectors work correctly (narrow to FileConnector for .sub_path)
    conns_a = bindings.get_connectors_for_resource("a")
    conns_b = bindings.get_connectors_for_resource("b")
    assert len(conns_a) == 1 and len(conns_b) == 1
    connector_a_loaded = conns_a[0]
    connector_b_loaded = conns_b[0]
    assert isinstance(connector_a_loaded, FileConnector)
    assert isinstance(connector_b_loaded, FileConnector)
    assert connector_a_loaded.sub_path is not None
    assert isinstance(connector_a_loaded.sub_path / "a", pathlib.Path)
    assert connector_b_loaded.sub_path is not None
    assert str(connector_b_loaded.sub_path / "a") == "a"

    assert connector_a_loaded.bound_source_kind() == BoundSourceKind.FILE
    assert connector_b_loaded.bound_source_kind() == BoundSourceKind.FILE


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
    )
    bindings.add_connector(file_connector)
    bindings.bind_resource("users", file_connector)

    # Add table pattern with date filter
    table_connector = TableConnector(
        table_name="events",
        schema_name="public",
        date_field="created_at",
        date_filter="> '2020-10-10'",
    )
    bindings.add_connector(table_connector)
    bindings.bind_resource("events", table_connector)

    # Verify connectors are stored correctly (narrow with isinstance checks)
    users_pattern = bindings.get_connectors_for_resource("users")[0]
    events_pattern = bindings.get_connectors_for_resource("events")[0]
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


def test_connector_hash_is_deterministic_for_defining_fields():
    connector_a = FileConnector(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        name="files_a",
    )
    connector_b = FileConnector(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        name="files_b",
    )
    assert connector_a.hash == connector_b.hash


def test_bindings_support_connector_resource_name_mapping():
    bindings = Bindings(
        connectors=[
            FileConnector(
                regex=r"^users.*\.csv$",
                sub_path=pathlib.Path("."),
                resource_name="users",
            )
        ]
    )
    conns = bindings.get_connectors_for_resource("users")
    assert len(conns) == 1
    assert isinstance(conns[0], FileConnector)
    assert conns[0].bound_source_kind() == BoundSourceKind.FILE


def test_bindings_support_top_level_resource_connector_objects():
    bindings = Bindings(
        connectors=[
            TableConnector(
                name="orders_table", table_name="orders", schema_name="public"
            ),
        ],
        resource_connector=[
            ResourceConnectorBinding(resource="orders", connector="orders_table")
        ],
    )
    conns = bindings.get_connectors_for_resource("orders")
    assert len(conns) == 1
    assert isinstance(conns[0], TableConnector)
    assert conns[0].table_name == "orders"
    assert conns[0].schema_name == "public"


def test_bindings_allow_multiple_resource_connector_mappings():
    bindings = Bindings(
        connectors=[
            FileConnector(
                name="users_files",
                regex=r"^users.*\.csv$",
                sub_path=pathlib.Path("."),
                resource_name="users",
            ),
            FileConnector(
                name="users_backup_files",
                regex=r"^users_backup.*\.csv$",
                sub_path=pathlib.Path("."),
            ),
        ],
        resource_connector=[
            ResourceConnectorBinding(resource="users", connector="users_backup_files")
        ],
    )
    conns = bindings.get_connectors_for_resource("users")
    assert len(conns) == 2
    by_name = {c.name for c in conns if isinstance(c, FileConnector)}
    assert by_name == {"users_files", "users_backup_files"}


def test_bindings_connector_connection_rejects_resource_key_as_connector_ref():
    """connector_connection.connector must name a connector (name or hash), not a resource."""
    with pytest.raises(ValueError, match="Unknown connector reference"):
        Bindings(
            connectors=[
                FileConnector(
                    name="openalex",
                    regex=r".*\.jsonl$",
                    sub_path=pathlib.Path("."),
                )
            ],
            resource_connector=[
                ResourceConnectorBinding(resource="work", connector="openalex")
            ],
            connector_connection=[{"connector": "work", "conn_proxy": "main"}],
        )


def test_bindings_resource_connector_accepts_dict_entries():
    bindings = Bindings(
        connectors=[
            FileConnector(
                name="openalex",
                regex=r".*\.jsonl$",
                sub_path=pathlib.Path("."),
            )
        ],
        resource_connector=[{"resource": "work", "connector": "openalex"}],
    )
    conns = bindings.get_connectors_for_resource("work")
    assert len(conns) == 1
    assert isinstance(conns[0], FileConnector)
    assert conns[0].name == "openalex"


def test_bindings_resource_connector_validation_error_message():
    with pytest.raises(
        ValueError, match=r"Invalid resource_connector entry at index 0"
    ):
        Bindings(
            connectors=[
                FileConnector(
                    name="openalex",
                    regex=r".*\.jsonl$",
                    sub_path=pathlib.Path("."),
                )
            ],
            resource_connector=[{"resource": "work"}],
        )
