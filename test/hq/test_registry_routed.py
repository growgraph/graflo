"""A table connector reads only the records its resource's routers keep."""

from __future__ import annotations

import logging
import pathlib
import sqlite3
from typing import Any

import pytest

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    FileConnector,
    TableConnector,
)
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.schema import Schema
from graflo.connections.onto import PostgresConfig
from graflo.connections.provider import InMemoryConnectionProvider
from graflo.data_source.sql import SQLConfig, SQLDataSource
from graflo.filter.select import SelectSpec
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.registry_builder import RegistryBuilder

LOGGER = "graflo.hq.registry_builder"
CLOSED = {"type_field": "kind", "type_map": {"srv": "server"}, "type_map_only": True}


def _schema() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "kg"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": name, "properties": ["id"], "identity": ["id"]}
                        for name in ("server", "switch", "rack")
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    )


#: A named transform writing a field other than the routed one.
LABEL = {
    "name": "label_of",
    "module": "builtins",
    "foo": "str",
    "input": ["name"],
    "output": ["label"],
}


def _model(*pipeline: dict[str, Any]) -> IngestionModel:
    model = IngestionModel.model_validate(
        {
            "resources": [{"name": "items", "pipeline": list(pipeline)}],
            "transforms": [LABEL],
        }
    )
    model.finish_init(_schema().core_schema)
    return model


def _router(**options: Any) -> dict[str, Any]:
    return {"vertex_router": {**CLOSED, **options}}


def _sources(
    connector: TableConnector | FileConnector | APIConnector,
    *pipeline: dict[str, Any],
    params: IngestionParams | None = None,
    strict: bool = True,
) -> list:
    bindings = Bindings(
        connectors=[connector],
        resource_connector=[{"resource": "items", "connector": "items"}],
        connector_connection=[{"connector": "items", "conn_proxy": "db"}]
        if isinstance(connector, TableConnector)
        else [],
    )
    provider = InMemoryConnectionProvider(
        {"db": PostgresConfig(uri="postgresql://localhost:5432", database="d")}
    )
    registry = RegistryBuilder(_schema(), _model(*pipeline)).build(
        bindings, params or IngestionParams(), provider, strict=strict
    )
    return registry.get_data_sources("items")


def _query(connector: TableConnector, *pipeline: dict[str, Any], **kw: Any) -> str:
    (source,) = _sources(connector, *pipeline, **kw)
    assert isinstance(source, SQLDataSource)
    return source.config.query


class TestTheQueryCarriesTheRoutedValues:
    def test_a_closed_router_narrows_a_plain_table(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connector = TableConnector(name="items", table_name="items")

        with caplog.at_level(logging.INFO, logger=LOGGER):
            query = _query(connector, _router())

        assert query == 'SELECT * FROM "public"."items" WHERE "kind" IN (\'srv\')'
        assert "reads only the records resource 'items' routes: kind in 1 value" in (
            caplog.text
        )

    def test_an_open_router_narrows_to_mapped_and_declared_names(self) -> None:
        connector = TableConnector(name="items", table_name="items")

        query = _query(connector, _router(type_map_only=False))

        assert query.endswith("WHERE \"kind\" IN ('rack', 'server', 'srv', 'switch')")

    def test_a_view_is_narrowed_on_the_joined_column(self) -> None:
        view = SelectSpec.model_validate(
            {
                "joins": [
                    {
                        "table": "cmdb_all_classes",
                        "alias": "ci_types",
                        "on_self": "cmdb_ci",
                        "on_other": "sys_id",
                    }
                ],
                "select": [
                    "all_base",
                    {"base": "cmdb_ci"},
                    {
                        "from_join": "ci_types",
                        "column": "sys_class_name",
                        "as": "ci_type",
                    },
                ],
            }
        )
        connector = TableConnector(name="items", table_name="cmdb_ci", view=view)

        query = _query(connector, _router(type_field="ci_type"))

        assert query.endswith("WHERE ci_types.\"sys_class_name\" IN ('srv')")

    def test_the_date_range_and_the_routed_values_both_apply(self) -> None:
        connector = TableConnector(name="items", table_name="items")

        query = _query(
            connector,
            _router(),
            params=IngestionParams(
                datetime_after="2020-06-01", datetime_column="updated"
            ),
        )

        assert query.endswith(
            "WHERE \"updated\" >= '2020-06-01' AND \"kind\" IN ('srv')"
        )

    def test_the_declared_manifest_is_left_alone(self) -> None:
        connector = TableConnector(name="items", table_name="items")
        before = connector.hash

        _query(connector, _router())

        assert connector.filters == []
        assert connector.hash == before


class TestTransformsBesideTheRouter:
    """Transforms at the router's level keep the read narrowed unless they write its field."""

    def test_merge_derived_keys_guarded_on_the_field_keep_the_filter(self) -> None:
        connector = TableConnector(name="items", table_name="items")
        derived = {
            "transform": {
                "call": {
                    "module": "graflo.util.transform",
                    "foo": "tagged_key",
                    "params": {"tag": "a", "sep": ":"},
                    "input": ["id"],
                    "output": ["server_key"],
                },
                "when": {"field": "kind", "in": ["srv"]},
            }
        }

        query = _query(connector, derived, _router())

        assert query.endswith("WHERE \"kind\" IN ('srv')")

    def test_a_named_transform_writing_elsewhere_keeps_the_filter(self) -> None:
        connector = TableConnector(name="items", table_name="items")

        query = _query(
            connector, {"transform": {"call": {"use": "label_of"}}}, _router()
        )

        assert query.endswith("WHERE \"kind\" IN ('srv')")

    def test_a_transform_rewriting_the_field_is_named_in_the_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connector = TableConnector(name="items", table_name="items")
        in_place = {
            "transform": {
                "call": {"module": "builtins", "foo": "str", "input": ["kind"]}
            }
        }

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            query = _query(connector, in_place, _router())

        assert "WHERE" not in query
        assert (
            "Connector 'items' reads every record of resource 'items': its routers "
            "cannot narrow the read, because step 0 (transform builtins.str): it "
            "writes ['kind']"
        ) in caplog.text


class TestNothingIsPushed:
    def test_a_static_vertex_step_leaves_the_query_whole(self) -> None:
        connector = TableConnector(name="items", table_name="items")

        query = _query(connector, _router(), {"vertex": "rack"})

        assert query == 'SELECT * FROM "public"."items"'

    def test_an_unresolved_field_under_a_closed_router_is_reported(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connector = TableConnector(
            name="items", table_name="items", select_columns=["*"]
        )

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            query = _query(connector, _router())

        assert "WHERE" not in query
        assert (
            "Connector 'items' reads every record, but resource 'items' keeps only "
            "records with kind in 1 value"
        ) in caplog.text

    def test_an_unresolved_field_under_an_open_router_is_quiet(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connector = TableConnector(
            name="items", table_name="items", select_columns=["*"]
        )

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            _query(connector, _router(type_map_only=False))

        assert "reads every record" not in caplog.text

    def test_a_router_routing_nothing_registers_no_source(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        connector = TableConnector(name="items", table_name="items")

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            sources = _sources(connector, _router(type_map={"pdu": "pdu"}))

        assert sources == []
        assert "Connector 'items' is not read" in caplog.text


class TestOtherConnectors:
    def test_a_file_under_a_closed_router_is_reported(
        self, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        (tmp_path / "items.csv").write_text("id,kind\n1,srv\n")
        connector = FileConnector(name="items", sub_path=tmp_path, regex=r"items\.csv")

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            _sources(connector, _router())

        assert "Connector 'items' reads every record" in caplog.text

    @pytest.mark.parametrize(
        "annotations,reported",
        [
            pytest.param({}, True, id="field-open"),
            pytest.param({"kind": "srv"}, False, id="field-fixed"),
        ],
    )
    def test_an_api_request_fixing_the_field_is_quiet(
        self,
        annotations: dict[str, str],
        reported: bool,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        connector = APIConnector(
            name="items", path="/items", row_annotations=annotations
        )

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            _sources(connector, _router(), strict=False)

        assert ("Connector 'items' reads every record" in caplog.text) is reported


def test_a_capped_read_returns_kept_rows_only(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "items.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE items (id INTEGER, kind TEXT)")
        db.executemany(
            "INSERT INTO items VALUES (?, ?)",
            [(1, "sw"), (2, "rk"), (3, "sw"), (4, "srv"), (5, "rk"), (6, "srv")],
        )
    connector = TableConnector(name="items", table_name="items", schema_name="main")
    query = _query(connector, _router())

    source = SQLDataSource(
        config=SQLConfig(connection_string=f"sqlite:///{path}", query=query)
    )
    rows = [row for batch in source.iter_batches(limit=2) for row in batch]

    assert rows == [{"id": 4, "kind": "srv"}, {"id": 6, "kind": "srv"}]
