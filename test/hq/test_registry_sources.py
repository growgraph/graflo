"""What RegistryBuilder hands each source: the run-time date range and file settings."""

from __future__ import annotations

import pathlib

from graflo.architecture.contract.bindings import (
    Bindings,
    ColumnTimeFilter,
    FileConnector,
    TableConnector,
)
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types.enums import EncodingType
from graflo.architecture.schema import Schema
from graflo.connections.onto import PostgresConfig
from graflo.connections.provider import InMemoryConnectionProvider
from graflo.data_source.file import TableFileDataSource
from graflo.data_source.sql import SQLDataSource
from graflo.filter import JoinClause
from graflo.filter.onto import ComparisonOperator, FilterExpression, LogicalOperator
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.registry_builder import RegistryBuilder


def _schema() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "kg"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": "event", "properties": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    )


def _ingestion_model(**resource: object) -> IngestionModel:
    model = IngestionModel.model_validate(
        {
            "resources": [
                {
                    "name": "events",
                    "pipeline": [{"vertex": "event", "from": {"id": "id"}}],
                    **resource,
                },
            ],
            "transforms": [],
        }
    )
    model.finish_init(_schema().core_schema)
    return model


def _provider() -> InMemoryConnectionProvider:
    return InMemoryConnectionProvider(
        postgres_by_resource={
            "events": PostgresConfig(
                uri="postgresql://localhost:5432", database="db", username="u"
            )
        }
    )


def _query(connector: TableConnector, params: IngestionParams) -> str:
    bindings = Bindings(
        connectors=[connector],
        resource_connector=[{"resource": "events", "connector": "events"}],
    )
    registry = RegistryBuilder(_schema(), _ingestion_model()).build(
        bindings, params, _provider(), strict=True
    )
    (source,) = registry.get_data_sources("events")
    assert isinstance(source, SQLDataSource)
    return source.config.query


def _june(datetime_column: str | None = None) -> IngestionParams:
    """A run over June 2020, optionally naming the date column itself."""
    return IngestionParams(
        datetime_after="2020-06-01",
        datetime_before="2020-07-01",
        datetime_column=datetime_column,
    )


class TestRunTimeDateRange:
    def test_applies_when_the_connector_names_the_date_column(self) -> None:
        connector = TableConnector(
            name="events",
            table_name="events",
            time_filter=ColumnTimeFilter(column="dt"),
        )

        assert _query(connector, _june()) == (
            'SELECT * FROM "public"."events" '
            "WHERE \"dt\" >= '2020-06-01' AND \"dt\" < '2020-07-01'"
        )

    def test_applies_with_the_column_from_the_run(self) -> None:
        connector = TableConnector(name="events", table_name="events")

        query = _query(connector, _june(datetime_column="dt"))

        assert query.endswith("WHERE \"dt\" >= '2020-06-01' AND \"dt\" < '2020-07-01'")

    def test_narrows_a_window_the_connector_already_declares(self) -> None:
        connector = TableConnector(
            name="events",
            table_name="events",
            time_filter=ColumnTimeFilter(column="dt", not_equals="2020-06-15"),
        )

        query = _query(connector, _june())

        assert query.endswith(
            "WHERE \"dt\" != '2020-06-15' "
            "AND \"dt\" >= '2020-06-01' AND \"dt\" < '2020-07-01'"
        )

    def test_is_qualified_with_the_base_alias_under_joins(self) -> None:
        connector = TableConnector(
            name="events",
            table_name="events",
            joins=[JoinClause(table="users", alias="u", on_self="uid", on_other="id")],
        )

        query = _query(connector, _june(datetime_column="dt"))

        assert query.endswith(
            "WHERE base.\"dt\" >= '2020-06-01' AND base.\"dt\" < '2020-07-01'"
        )

    def test_an_or_filter_beside_the_range_is_parenthesised(self) -> None:
        def leaf(field: str) -> FilterExpression:
            return FilterExpression(
                kind="leaf",
                field=field,
                cmp_operator=ComparisonOperator.EQ,
                value=[1],
            )

        connector = TableConnector(
            name="events",
            table_name="events",
            filters=[
                FilterExpression(
                    kind="composite",
                    operator=LogicalOperator.OR,
                    deps=[leaf("a"), leaf("b")],
                )
            ],
        )

        query = _query(connector, _june(datetime_column="dt"))

        assert query.endswith(
            'WHERE ("a" = 1 OR "b" = 1) '
            "AND \"dt\" >= '2020-06-01' AND \"dt\" < '2020-07-01'"
        )

    def test_no_range_leaves_the_query_alone(self) -> None:
        connector = TableConnector(
            name="events",
            table_name="events",
            time_filter=ColumnTimeFilter(column="dt"),
        )

        assert _query(connector, IngestionParams()) == 'SELECT * FROM "public"."events"'


class TestFileSources:
    def _source(
        self, tmp_path: pathlib.Path, filename: str, **resource: object
    ) -> TableFileDataSource:
        (tmp_path / filename).write_text("id\n1\n")
        bindings = Bindings(
            connectors=[
                FileConnector(name="events", sub_path=tmp_path, resource_name="events")
            ],
        )
        registry = RegistryBuilder(_schema(), _ingestion_model(**resource)).build(
            bindings, IngestionParams(), strict=True
        )
        (source,) = registry.get_data_sources("events")
        assert isinstance(source, TableFileDataSource)
        return source

    def test_a_tsv_file_is_read_with_a_tab_separator(
        self, tmp_path: pathlib.Path
    ) -> None:
        assert self._source(tmp_path, "events.tsv").sep == "\t"

    def test_a_csv_file_is_read_with_a_comma_separator(
        self, tmp_path: pathlib.Path
    ) -> None:
        assert self._source(tmp_path, "events.csv").sep == ","

    def test_the_resource_encoding_reaches_the_file_source(
        self, tmp_path: pathlib.Path
    ) -> None:
        source = self._source(tmp_path, "events.csv", encoding="ISO-8859-1")

        assert source.encoding == EncodingType.ISO_8859
