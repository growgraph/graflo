from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema import Schema
from graflo.hq.caster import IngestionParams
from graflo.hq.registry_builder import RegistryBuilder


def _minimal_schema() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "kg"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": "a", "fields": ["id"], "identity": ["id"]},
                        {"name": "b", "fields": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    )


def test_manifest_minimal_canonical_roundtrip_is_idempotent() -> None:
    cfg = {
        "schema": {
            "metadata": {"name": "kg", "version": "1.0.0", "description": None},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "fields": ["id"], "identity": ["id"]}
                    ]
                },
                "edge_config": {"edges": []},
            },
        },
        "ingestion_model": {
            "resources": [
                {
                    "name": "people",
                    "apply": [{"vertex": "person"}],
                }
            ]
        },
    }
    manifest = GraphManifest.from_config(cfg)
    minimal = manifest.to_minimal_canonical_dict()

    # Canonical style should normalize aliases and omit defaults/None.
    assert "schema" in minimal
    assert minimal["ingestion_model"]["resources"][0]["pipeline"] == [
        {"vertex": "person"}
    ]
    assert "apply" not in minimal["ingestion_model"]["resources"][0]
    assert "description" not in minimal["schema"]["metadata"]
    assert "infer_edges" not in minimal["ingestion_model"]["resources"][0]
    assert "encoding" not in minimal["ingestion_model"]["resources"][0]

    minimal_rt = GraphManifest.from_config(minimal).to_minimal_canonical_dict()
    assert minimal_rt == minimal


def test_ingestion_model_strict_transform_reference_fails_fast() -> None:
    schema = _minimal_schema()
    ingestion_model = IngestionModel.model_validate(
        {
            "resources": [
                {
                    "name": "r1",
                    "pipeline": [
                        {"transform": {"call": {"use": "missing_transform"}}},
                        {"vertex": "a"},
                    ],
                }
            ],
            "transforms": [],
        }
    )

    try:
        ingestion_model.finish_init(schema.core_schema, strict_references=True)
        assert False, "Expected strict transform reference validation to fail"
    except ValueError as exc:
        assert "was not found in ingestion_model.transforms" in str(exc)


def test_registry_builder_strict_mode_aggregates_missing_connectors() -> None:
    schema = _minimal_schema()
    ingestion_model = IngestionModel.model_validate(
        {"resources": [{"name": "r1", "pipeline": [{"vertex": "a"}]}], "transforms": []}
    )
    ingestion_model.finish_init(schema.core_schema)

    builder = RegistryBuilder(schema, ingestion_model)
    params = IngestionParams()
    bindings = Bindings()

    try:
        builder.build(bindings, params, strict=True)
        assert False, "Expected strict registry build to fail"
    except ValueError as exc:
        assert "Registry build failed in strict mode" in str(exc)
        assert "No resource type found for resource 'r1'" in str(exc)


def test_resource_finish_init_does_not_mutate_shared_schema_edge_config() -> None:
    schema = _minimal_schema()
    ingestion_model = IngestionModel.model_validate(
        {
            "resources": [
                {"name": "r1", "pipeline": [{"edge": {"from": "a", "to": "b"}}]},
                {"name": "r2", "pipeline": [{"edge": {"from": "a", "to": "b"}}]},
            ],
            "transforms": [],
        }
    )

    assert len(schema.core_schema.edge_config.edges) == 0
    ingestion_model.finish_init(schema.core_schema)

    # Shared logical schema stays untouched.
    assert len(schema.core_schema.edge_config.edges) == 0
    # Runtime resource edge configs receive local dynamic edge registrations.
    assert len(ingestion_model.resources[0].edge_config.edges) == 1
    assert len(ingestion_model.resources[1].edge_config.edges) == 1


def test_bindings_reject_inline_credentials_payload() -> None:
    try:
        Bindings.from_dict(
            {
                "postgres_connections": {"db1": {"host": "localhost"}},
                "file_connectors": {},
            }
        )
        assert False, "Expected inline credentials to be rejected"
    except ValueError as exc:
        assert "Inline credential payload is not supported in Bindings" in str(exc)
