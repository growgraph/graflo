from __future__ import annotations

import pathlib

import pytest

from graflo.architecture.contract.bindings import Bindings, FileConnector
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.schema import Schema
from graflo.hq.caster import Caster
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.registry_builder import RegistryBuilder


def _minimal_schema() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "kg"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": "user", "properties": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    )


def _users_ingestion_model() -> IngestionModel:
    ingestion_model = IngestionModel.model_validate(
        {
            "resources": [
                {
                    "name": "users",
                    "pipeline": [{"vertex": "user", "from": {"id": "id"}}],
                },
            ],
            "transforms": [],
        }
    )
    ingestion_model.finish_init(_minimal_schema().core_schema)
    return ingestion_model


def _two_file_connectors(tmp_path: pathlib.Path) -> Bindings:
    (tmp_path / "users_a.csv").write_text("id\n1\n")
    (tmp_path / "users_b.csv").write_text("id\n2\n")
    return Bindings(
        connectors=[
            FileConnector(
                name="users_a",
                regex=r"^users_a\.csv$",
                sub_path=tmp_path,
                resource_name="users",
            ),
            FileConnector(
                name="users_b",
                regex=r"^users_b\.csv$",
                sub_path=tmp_path,
            ),
        ],
        resource_connector=[{"resource": "users", "connector": "users_b"}],
    )


def test_registry_builder_filters_connectors(tmp_path: pathlib.Path) -> None:
    schema = _minimal_schema()
    ingestion_model = _users_ingestion_model()
    bindings = _two_file_connectors(tmp_path)
    builder = RegistryBuilder(schema, ingestion_model)

    registry = builder.build(
        bindings,
        IngestionParams(connectors=["users_a"]),
    )
    sources = registry.get_data_sources("users")
    assert len(sources) == 1


def test_registry_builder_connectors_filter_accepts_connector_hash(
    tmp_path: pathlib.Path,
) -> None:
    schema = _minimal_schema()
    ingestion_model = _users_ingestion_model()
    bindings = _two_file_connectors(tmp_path)
    connector_hash = bindings.connectors[0].hash
    builder = RegistryBuilder(schema, ingestion_model)

    registry = builder.build(
        bindings,
        IngestionParams(connectors=[connector_hash]),
    )
    assert len(registry.get_data_sources("users")) == 1


def test_registry_builder_resources_and_connectors_intersect(
    tmp_path: pathlib.Path,
) -> None:
    schema = _minimal_schema()
    ingestion_model = IngestionModel.model_validate(
        {
            "resources": [
                {
                    "name": "users",
                    "pipeline": [{"vertex": "user", "from": {"id": "id"}}],
                },
                {
                    "name": "events",
                    "pipeline": [{"vertex": "user", "from": {"id": "id"}}],
                },
            ],
            "transforms": [],
        }
    )
    ingestion_model.finish_init(schema.core_schema)
    (tmp_path / "users_a.csv").write_text("id\n1\n")
    (tmp_path / "events_a.csv").write_text("id\n2\n")
    bindings = Bindings(
        connectors=[
            FileConnector(
                name="users_a",
                regex=r"^users_a\.csv$",
                sub_path=tmp_path,
                resource_name="users",
            ),
            FileConnector(
                name="events_a",
                regex=r"^events_a\.csv$",
                sub_path=tmp_path,
                resource_name="events",
            ),
        ],
    )
    builder = RegistryBuilder(schema, ingestion_model)

    registry = builder.build(
        bindings,
        IngestionParams(resources=["users"], connectors=["events_a"]),
    )
    assert registry.get_data_sources("users") == []
    assert len(registry.get_data_sources("events")) == 0


def test_registry_builder_connectors_filter_does_not_warn_when_all_filtered(
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = _minimal_schema()
    ingestion_model = _users_ingestion_model()
    bindings = _two_file_connectors(tmp_path)
    builder = RegistryBuilder(schema, ingestion_model)

    registry = builder.build(
        bindings,
        IngestionParams(connectors=["users_a"]),
        strict=True,
    )
    assert len(registry.get_data_sources("users")) == 1
    assert "No connectors bound for resource 'users'" not in caplog.text


def test_caster_resolve_ingestion_scope_rejects_unknown_connector() -> None:
    schema = _minimal_schema()
    ingestion_model = _users_ingestion_model()
    caster = Caster(schema, ingestion_model)

    with pytest.raises(ValueError, match="Unknown connector reference 'missing'"):
        caster._resolve_ingestion_scope(
            IngestionParams(connectors=["missing"]),
            bindings=Bindings(),
        )
