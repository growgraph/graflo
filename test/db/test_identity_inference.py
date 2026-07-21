"""Tests for algorithmic identity inference."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import random

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.graflo_output import GraFloOutput
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.identity_inference import (
    DEFAULT_MIN_SAMPLE_SIZE,
    IdentityInferenceConfig,
    IdentityInferencer,
    apply_identity_inference_to_vertices,
    bootstrap_is_stable,
    bootstrap_pass_rate,
    compute_hash_identity,
    infer_column_type_cost,
    infer_identities_from_snapshot,
    score_candidate,
    uniqueness_ratio,
)
from graflo.db.connection import ArangoConfig
from graflo.hq.db_writer import DBWriter
from graflo.onto import DBType


def _person_samples(*, count: int = 100, include_uuid: bool = True) -> list[dict]:
    samples = [
        {"name": f"Person {index}", "email": f"p{index}@example.com"}
        for index in range(count)
    ]
    if include_uuid:
        for index, sample in enumerate(samples):
            sample["user_id"] = f"{index:08d}-0000-4000-8000-000000000000"
    return samples


def _small_sample_config() -> IdentityInferenceConfig:
    return IdentityInferenceConfig(min_sample_size=10)


def test_infer_column_type_cost_handles_common_types() -> None:
    assert infer_column_type_cost([1, 2, 3]) == 0.0
    assert infer_column_type_cost(["a", "b"]) == 0.1
    assert infer_column_type_cost(["00000000-0000-4000-8000-000000000000"]) == 0.0
    assert infer_column_type_cost([datetime(2024, 1, 1)]) == 0.5
    assert infer_column_type_cost([1.5, 2.5]) == 1.0
    assert infer_column_type_cost(["2024-01-01T10:00:00Z"]) == 0.5
    assert infer_column_type_cost([["a", "b"], ["c"]]) is None
    assert infer_column_type_cost([{"a": 1}]) is None


def test_infer_column_type_cost_rejects_majority_none_and_long_text() -> None:
    assert infer_column_type_cost([None, None, None, "x"]) is None
    assert infer_column_type_cost(["x" * 300]) is None


def test_score_candidate_prefers_semantic_and_narrow_keys() -> None:
    type_costs = {"name": 0.1, "user_id": 0.0}
    semantic_score = score_candidate(["user_id"], type_costs)
    wide_score = score_candidate(["name", "user_id"], type_costs)
    assert semantic_score < wide_score


def test_bootstrap_pass_rate_requires_minimum_sample_size() -> None:
    samples = [{"id": index} for index in range(3)]
    assert bootstrap_pass_rate(samples, ["id"], min_sample_size=10) == 0.0
    assert bootstrap_is_stable(samples, ["id"], min_sample_size=10) is False


def test_bootstrap_is_stable_accepts_stable_column() -> None:
    samples = [{"user_id": index} for index in range(12)]
    assert bootstrap_is_stable(
        samples,
        ["user_id"],
        min_sample_size=10,
        rng=random.Random(0),
    )


def test_vertex_identity_mode_natural_hash_and_blank() -> None:
    natural_unary = Vertex(
        name="a",
        properties=[Field(name="code")],
        identity=["code"],
    )
    natural_composite = Vertex(
        name="b",
        properties=[Field(name="org"), Field(name="slug")],
        identity=["org", "slug"],
    )
    hash_vertex = Vertex(
        name="c",
        properties=[Field(name="org"), Field(name="slug")],
        hash_identity_properties=["org", "slug"],
    )
    blank_vertex = Vertex(name="d", properties=[], blank=True)

    assert natural_unary.identity_mode == "natural"
    assert natural_composite.identity_mode == "natural"
    assert hash_vertex.identity_mode == "hash"
    assert blank_vertex.identity_mode == "blank"


def test_vertex_config_vertices_by_identity_mode() -> None:
    config = VertexConfig(
        vertices=[
            Vertex(name="n", properties=[Field(name="id")], identity=["id"]),
            Vertex(
                name="h",
                properties=[Field(name="a"), Field(name="b")],
                hash_identity_properties=["a", "b"],
            ),
            Vertex(name="b", properties=[], blank=True),
        ]
    )
    assert config.vertices_by_identity_mode("natural") == ["n"]
    assert config.vertices_by_identity_mode("hash") == ["h"]
    assert config.vertices_by_identity_mode("blank") == ["b"]


def test_apply_identity_inference_to_vertices() -> None:
    product_samples = [
        {"org": index % 3, "slug": index // 3, "status": "active"}
        for index in range(12)
    ]
    supplier_samples = [{"supplier_code": f"SUP-{index:04d}"} for index in range(12)]
    vertices = [
        Vertex(
            name="product",
            properties=[
                Field(name="org"),
                Field(name="slug"),
                Field(name="status"),
            ],
        ),
        Vertex(
            name="supplier",
            properties=[Field(name="supplier_code")],
        ),
    ]
    config = _small_sample_config()
    updated, results = apply_identity_inference_to_vertices(
        vertices,
        {"product": product_samples, "supplier": supplier_samples},
        config=config,
    )
    assert len(updated) == 2
    assert results["product"].strategy == "composite"
    assert results["supplier"].strategy == "unary"
    assert updated[0].identity_mode == "natural"
    assert set(updated[0].identity) == {"org", "slug"}
    assert updated[1].identity == ["supplier_code"]


def test_identity_inference_config_default_min_sample_size() -> None:
    config = IdentityInferenceConfig()
    assert config.min_sample_size == DEFAULT_MIN_SAMPLE_SIZE
    assert config.max_sample_size is None


def test_identity_inferencer_unary_scan() -> None:
    inferencer = IdentityInferencer(
        config=IdentityInferenceConfig(min_sample_size=100),
        rng=random.Random(0),
    )
    result = inferencer.infer(_person_samples(count=100))
    assert result.strategy == "unary"
    assert result.identity == ["user_id"]
    assert result.hash_identity_properties == []
    assert result.confidence == 1.0


def test_identity_inferencer_composite_key() -> None:
    samples = [
        {"org": index % 3, "slug": index // 3, "status": "active"}
        for index in range(12)
    ]
    inferencer = IdentityInferencer(
        config=_small_sample_config(),
        rng=random.Random(1),
    )
    result = inferencer.infer(samples, property_names=["org", "slug", "status"])
    assert result.strategy == "composite"
    assert set(result.identity) == {"org", "slug"}
    assert result.hash_identity_properties == []


def test_identity_inferencer_hash_fallback_when_key_too_wide() -> None:
    samples = [{"org": index % 4, "slug": index // 4} for index in range(16)]
    inferencer = IdentityInferencer(
        config=_small_sample_config().model_copy(update={"max_key_width": 1}),
        rng=random.Random(2),
    )
    result = inferencer.infer(samples, property_names=["org", "slug"])
    assert result.strategy == "hash_fallback"
    assert result.identity == ["id"]
    assert set(result.hash_identity_properties) == {"org", "slug"}
    assert result.warning is not None


def test_identity_inferencer_no_viable_identity_when_sample_too_small() -> None:
    inferencer = IdentityInferencer(config=IdentityInferenceConfig())
    result = inferencer.infer([{"id": 1}])
    assert result.strategy == "no_viable_identity"
    assert result.warning == "sample too small"


def test_identity_inferencer_no_viable_identity_when_all_columns_disqualified() -> None:
    samples = [{"blob": "x" * 300, "mostly_null": None} for _ in range(12)]
    inferencer = IdentityInferencer(config=_small_sample_config())
    result = inferencer.infer(samples, property_names=["blob", "mostly_null"])
    assert result.strategy == "no_viable_identity"
    assert result.warning == "all columns disqualified"


def test_identity_inferencer_no_viable_identity_when_no_unique_combination() -> None:
    samples = [{"group": index % 4} for index in range(12)]
    inferencer = IdentityInferencer(config=_small_sample_config())
    result = inferencer.infer(samples, property_names=["group"])
    assert result.strategy == "no_viable_identity"
    assert result.warning == "no unique combination found"


def test_compute_hash_identity_is_stable() -> None:
    doc = {"org": "acme", "slug": "widget", "status": "active"}
    first = compute_hash_identity(doc, ["org", "slug"])
    second = compute_hash_identity(doc, ["org", "slug"])
    assert first == second
    assert len(first) == 64


def test_vertex_config_hash_identity_normalization() -> None:
    config = VertexConfig(
        vertices=[
            Vertex(
                name="product",
                properties=[Field(name="org"), Field(name="slug")],
                hash_identity_properties=["org", "slug"],
            )
        ]
    )
    vertex = config.vertices[0]
    assert vertex.identity == ["id"]
    assert vertex.hash_identity_properties == ["org", "slug"]
    assert "product" in config.hash_identity_vertices


def test_assign_hash_identity_ids_writes_deterministic_key(monkeypatch) -> None:
    class _FakeDB:
        def __init__(self) -> None:
            self.upsert_calls: list[tuple[list[dict], str, list[str]]] = []

        def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
            self.upsert_calls.append((docs, class_name, list(match_keys)))

    class _FakeConnectionManager:
        db = _FakeDB()

        def __init__(self, connection_config):
            self.connection_config = connection_config

        def __enter__(self):
            return self.db

        def __exit__(self, exc_type, exc, tb):
            return False

    vertex_config = VertexConfig(
        vertices=[
            Vertex(
                name="product",
                properties=[Field(name="org"), Field(name="slug")],
                hash_identity_properties=["org", "slug"],
            )
        ]
    )
    schema = Schema(
        metadata=GraphMetadata(name="test"),
        core_schema=CoreSchema(
            vertex_config=vertex_config, edge_config=EdgeConfig(edges=[])
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )
    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=ingestion_model, dry=False)
    gc = GraphContainer(
        vertices={"product": [{"org": "acme", "slug": "widget"}]},
        edges={},
        linear=[],
    )

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _FakeConnectionManager)
    conn_conf = ArangoConfig(uri="http://localhost:8529", username="root", password="x")
    asyncio.run(writer._push_vertices(gc, conn_conf))

    doc = gc.vertices["product"][0]
    expected = compute_hash_identity(doc, ["org", "slug"])
    assert doc["id"] == expected


def test_infer_identities_from_snapshot_returns_new_object(tmp_path: Path) -> None:
    samples = _person_samples()
    vertex = Vertex(
        name="person",
        properties=[Field(name="name"), Field(name="email"), Field(name="user_id")],
    )
    vertex_config = VertexConfig(vertices=[vertex])
    schema = Schema(
        metadata=GraphMetadata(name="snapshot"),
        core_schema=CoreSchema(
            vertex_config=vertex_config,
            edge_config=EdgeConfig(edges=[]),
        ),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )
    original = GraFloOutput(
        graph_schema=schema,
        data=GraphContainer(vertices={"person": samples}, edges={}, linear=[]),
    )
    original_identity = list(original.core_schema.vertex_config.vertices[0].identity)
    snapshot_path = tmp_path / "snapshot.yaml"
    original.to_yaml(str(snapshot_path))

    updated = infer_identities_from_snapshot(
        snapshot_path,
        output_path=tmp_path / "updated.yaml",
        config=_small_sample_config(),
    )

    assert updated is not original
    assert (
        list(original.core_schema.vertex_config.vertices[0].identity)
        == original_identity
    )
    updated_vertex = updated.core_schema.vertex_config.vertices[0]
    assert updated_vertex.identity == ["user_id"]
    assert (tmp_path / "updated.yaml").exists()


def test_identity_inferencer_max_sample_size_caps_large_dataset() -> None:
    samples = _person_samples(count=5000)
    inferencer = IdentityInferencer(
        config=IdentityInferenceConfig(min_sample_size=100, max_sample_size=200),
        rng=random.Random(0),
    )
    result = inferencer.infer(samples)
    assert result.strategy == "unary"
    assert result.identity == ["user_id"]


def test_uniqueness_ratio() -> None:
    samples = [{"a": 1}, {"a": 2}, {"a": 1}]
    assert uniqueness_ratio(samples, ["a"]) == pytest.approx(2 / 3)
