import pathlib

import pytest
from pydantic import ValidationError

from graflo.architecture.contract.bindings import (
    Bindings,
    BoundSourceKind,
    ColumnTimeFilter,
    ConnectorUpdate,
    FileConnector,
    ResourceConnectorBinding,
    TableConnector,
)


def _tc(d: dict[str, object]) -> TableConnector:
    return TableConnector.model_validate(d)


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
    assert pattern.time_filter is None
    assert pattern.date_field is None


def test_file_connector_rejects_unknown_time_keys() -> None:
    """Legacy flat date_* keys are not accepted (extra=forbid)."""
    with pytest.raises(ValidationError):
        FileConnector.model_validate(
            {
                "regex": r".*\.csv$",
                "sub_path": pathlib.Path("./data"),
                "date_filter": "> '2020-10-10'",
            }
        )


def test_file_connector_time_filter() -> None:
    pattern = FileConnector(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
        time_filter=ColumnTimeFilter(
            column="dt",
            start="2020-10-10",
            start_inclusive=False,
        ),
    )
    assert pattern.date_field == "dt"
    assert pattern.time_filter is not None
    assert pattern.time_filter.column == "dt"
    assert pattern.time_filter.start == "2020-10-10"
    assert pattern.time_filter.start_inclusive is False


def test_table_connector_basic():
    """Test TableConnector basic functionality."""
    pattern = TableConnector(
        table_name="events",
        schema_name="public",
    )
    assert pattern.table_name == "events"
    assert pattern.schema_name == "public"


def test_table_connector_rejects_unknown_time_keys() -> None:
    with pytest.raises(ValidationError):
        TableConnector.model_validate(
            {"table_name": "events", "date_filter": "> '2020-10-10'"}
        )


def test_table_connector_time_filter_from_dict() -> None:
    pattern = _tc(
        {
            "table_name": "events",
            "time_filter": {
                "column": "dt",
                "start": "2020-10-10",
                "start_inclusive": False,
            },
        }
    )
    assert pattern.date_field == "dt"
    assert pattern.time_filter is not None
    assert pattern.time_filter.column == "dt"
    assert pattern.time_filter.start == "2020-10-10"
    assert pattern.time_filter.start_inclusive is False


def test_table_connector_build_where_clause_no_filter():
    """Test build_where_clause with no filters."""
    pattern = TableConnector(table_name="events")
    assert pattern.build_where_clause() == ""


def test_table_connector_build_where_clause_time_bounds():
    """Test build_where_clause with strict lower bound."""
    pattern = TableConnector(
        table_name="events",
        time_filter=ColumnTimeFilter(
            column="created_at",
            start="2020-10-10",
            start_inclusive=False,
        ),
    )
    where_clause = pattern.build_where_clause()
    assert '"created_at"' in where_clause
    assert "> '2020-10-10'" in where_clause

    pattern2 = TableConnector(
        table_name="events",
        time_filter=ColumnTimeFilter(
            column="dt",
            start="2020-10-10",
            start_inclusive=False,
        ),
    )
    where_clause2 = pattern2.build_where_clause()
    assert '"dt"' in where_clause2
    assert "> '2020-10-10'" in where_clause2

    pattern3 = TableConnector(
        table_name="events",
        time_filter=ColumnTimeFilter(
            column="dt",
            start="2015-11-11",
            start_inclusive=True,
        ),
    )
    where_clause3 = pattern3.build_where_clause()
    assert '"dt"' in where_clause3
    assert ">= '2015-11-11'" in where_clause3


def test_table_connector_build_where_clause_interval_range():
    """Test build_where_clause with start + interval (half-open window)."""
    pattern = TableConnector(
        table_name="transactions",
        time_filter=ColumnTimeFilter(
            column="dt",
            start="2015-11-11",
            interval="30D",
        ),
    )
    where_clause = pattern.build_where_clause()
    assert '"dt"' in where_clause
    assert ">= '2015-11-11'" in where_clause
    assert "< '2015-12-11'" in where_clause
    assert "AND" in where_clause
    assert where_clause.count(">=") == 1
    assert where_clause.count("<") == 1


def test_table_connector_build_where_clause_neq_and_upper():
    """Test build_where_clause with != and upper bound."""
    connector = TableConnector(
        table_name="events",
        time_filter=ColumnTimeFilter(
            column="timestamp",
            end="2023-01-01",
            end_inclusive=False,
        ),
    )
    where1 = connector.build_where_clause()
    assert '"timestamp"' in where1
    assert "< '2023-01-01'" in where1

    connector2 = TableConnector(
        table_name="events",
        time_filter=ColumnTimeFilter(column="dt", not_equals="2020-01-01"),
    )
    where2 = connector2.build_where_clause()
    assert '"dt"' in where2
    assert "!= '2020-01-01'" in where2


def test_connectors_with_filtering():
    """Test Bindings collection with filtering parameters."""
    bindings = Bindings()

    file_connector = FileConnector(
        regex=r".*\.csv$",
        sub_path=pathlib.Path("./data"),
    )
    bindings.add_connector(file_connector)
    bindings.bind_resource("users", file_connector)

    table_connector = TableConnector(
        table_name="events",
        schema_name="public",
        time_filter=ColumnTimeFilter(
            column="created_at",
            start="2020-10-10",
            start_inclusive=False,
        ),
    )
    bindings.add_connector(table_connector)
    bindings.bind_resource("events", table_connector)

    users_pattern = bindings.get_connectors_for_resource("users")[0]
    events_pattern = bindings.get_connectors_for_resource("events")[0]
    assert isinstance(users_pattern, FileConnector)
    assert users_pattern.regex == r".*\.csv$"
    assert isinstance(events_pattern, TableConnector)
    assert events_pattern.date_field == "created_at"
    tf = events_pattern.time_filter
    assert tf is not None
    assert tf.column == "created_at"
    assert tf.start == "2020-10-10"
    assert tf.start_inclusive is False


def test_table_connector_sql_query_building():
    """Test that TableConnector builds correct SQL queries with filters."""
    table_connector = TableConnector(
        table_name="events",
        schema_name="public",
        time_filter=ColumnTimeFilter(
            column="dt",
            start="2020-10-10",
            start_inclusive=False,
        ),
    )

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
        time_filter=ColumnTimeFilter(
            column="dt",
            start="2015-11-11",
            interval="30D",
        ),
    )

    where_clause = pattern.build_where_clause()
    assert "AND" in where_clause
    assert ">=" in where_clause
    assert "<" in where_clause
    assert "2015-11-11" in where_clause
    assert "2015-12-11" in where_clause

    base_query = 'SELECT * FROM "public"."transactions"'
    full_query = f"{base_query} WHERE {where_clause}"
    assert "WHERE" in full_query
    assert where_clause.count("AND") == 1


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


def _events_table_with_range() -> TableConnector:
    return TableConnector(
        name="events_table",
        table_name="events",
        time_filter=ColumnTimeFilter(
            column="created_at",
            start="2020-01-01",
            interval="365D",
        ),
    )


def test_replace_connector_rewires_hashes() -> None:
    bindings = Bindings(
        connectors=[_events_table_with_range()],
        resource_connector=[
            ResourceConnectorBinding(resource="events", connector="events_table")
        ],
    )
    old = bindings.get_connectors_for_resource("events")[0]
    assert isinstance(old, TableConnector)
    old_hash = old.hash
    merged = old.model_dump(mode="python")
    merged["time_filter"] = {
        "column": "created_at",
        "start": "2021-06-01",
        "interval": "30D",
    }
    new = TableConnector.model_validate(merged)
    assert new.hash != old_hash
    bindings.replace_connector(old, new)
    loaded = bindings.get_connectors_for_resource("events")[0]
    assert isinstance(loaded, TableConnector)
    assert loaded.hash == new.hash
    tf = loaded.time_filter
    assert tf is not None
    assert tf.start == "2021-06-01"
    assert tf.interval == "30D"
    assert loaded.table_name == "events"


def test_replace_connector_preserves_conn_proxy() -> None:
    bindings = Bindings(
        connectors=[_events_table_with_range()],
        resource_connector=[
            ResourceConnectorBinding(resource="events", connector="events_table")
        ],
        connector_connection=[
            {"connector": "events_table", "conn_proxy": "pg_main"},
        ],
    )
    old = bindings.get_connectors_for_resource("events")[0]
    assert isinstance(old, TableConnector)
    merged = old.model_dump(mode="python")
    merged["time_filter"] = {
        "column": "created_at",
        "start": "2021-06-01",
        "interval": "30D",
    }
    new = TableConnector.model_validate(merged)
    bindings.replace_connector(old, new)
    assert bindings.get_conn_proxy_for_connector(new) == "pg_main"


def test_apply_connector_update_partial() -> None:
    bindings = Bindings(
        connectors=[_events_table_with_range()],
        resource_connector=[
            ResourceConnectorBinding(resource="events", connector="events_table")
        ],
    )
    update = ConnectorUpdate.model_validate(
        {
            "connector": "events_table",
            "time_filter": {
                "column": "created_at",
                "start": "2021-06-01",
                "interval": "30D",
            },
        }
    )
    assert update.as_patch() == {
        "time_filter": {
            "column": "created_at",
            "start": "2021-06-01",
            "interval": "30D",
        },
    }
    bindings.apply_connector_update(update)
    loaded = bindings.get_connectors_for_resource("events")[0]
    assert isinstance(loaded, TableConnector)
    tf = loaded.time_filter
    assert tf is not None
    assert tf.start == "2021-06-01"
    assert tf.interval == "30D"
    assert loaded.table_name == "events"
    assert loaded.date_field == "created_at"


def test_apply_external_connector_updates_after_manifest_load() -> None:
    """Patches are not part of the manifest; apply after Bindings is built."""
    bindings = Bindings.model_validate(
        {
            "connectors": [
                {
                    "name": "events_table",
                    "table_name": "events",
                    "time_filter": {
                        "column": "created_at",
                        "start": "2020-01-01",
                        "interval": "365D",
                    },
                }
            ],
            "resource_connector": [
                {"resource": "events", "connector": "events_table"},
            ],
        }
    )
    external_patches = [
        {
            "connector": "events_table",
            "time_filter": {
                "column": "created_at",
                "start": "2021-06-01",
                "interval": "30D",
            },
        },
    ]
    for row in external_patches:
        bindings.apply_connector_update(ConnectorUpdate.model_validate(row))
    loaded = bindings.get_connectors_for_resource("events")[0]
    assert isinstance(loaded, TableConnector)
    tf = loaded.time_filter
    assert tf is not None
    assert tf.start == "2021-06-01"
    assert tf.interval == "30D"
    assert loaded.table_name == "events"


def test_table_connector_time_filter_pandas_interval_hours() -> None:
    pattern = TableConnector(
        table_name="events",
        time_filter=ColumnTimeFilter(
            column="ts",
            start="2024-01-01T10:00:00",
            interval="2h",
        ),
    )
    where_clause = pattern.build_where_clause()
    assert '"ts"' in where_clause
    assert ">=" in where_clause
    assert "<" in where_clause
    assert "2024-01-01 10:00:00" in where_clause


def test_column_time_filter_invalid_interval() -> None:
    with pytest.raises(ValueError, match="Invalid pandas timedelta"):
        ColumnTimeFilter(column="ts", start="2020-01-01", interval="not_a_timedelta")


def test_api_connector_basic() -> None:
    from graflo.architecture.contract.bindings import APIConnector, PaginationConfig

    connector = APIConnector(
        name="users_api",
        path="/api/users",
        pagination=PaginationConfig(page_size=50),
    )
    assert connector.bound_source_kind() == BoundSourceKind.API
    assert connector.matches("users_api")
    assert connector.matches("users")


def test_api_connector_build_api_config() -> None:
    from graflo.architecture.contract.bindings import APIConnector
    from graflo.hq.connection_provider import ApiAuth

    connector = APIConnector(path="/v1/items", method="GET")
    config = connector.build_api_config(
        base_url="https://api.example.com",
        auth=ApiAuth(auth_type="bearer", token="secret"),
        default_headers={"Accept": "application/json"},
    )
    assert config.url == "https://api.example.com/v1/items"
    assert config.auth is not None
    assert config.auth.token == "secret"
    assert config.headers["Accept"] == "application/json"
