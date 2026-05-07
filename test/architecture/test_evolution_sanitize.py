"""Tests for :mod:`graflo.architecture.evolution` sanitize / rename ops."""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    RenameVertexFieldsOp,
    SanitizeOp,
    apply_evolution,
    apply_rename_vertex_fields,
    apply_sanitize,
)
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig, FieldType
from graflo.hq.sanitizer import Sanitizer
from graflo.onto import DBType


def _build_manifest(
    *,
    pipeline_a: list[dict] | None = None,
    pipeline_b: list[dict] | None = None,
    user_properties: list[Field] | None = None,
    user_identity: list[str] | None = None,
) -> GraphManifest:
    meta = GraphMetadata(name="rename_fields", version="1.0.0")
    user_props = user_properties or [
        Field(name="id"),
        Field(name="user-name"),
    ]
    identity = user_identity or ["id"]
    vc = VertexConfig(
        vertices=[
            Vertex(name="users", properties=user_props, identity=identity),
            Vertex(name="orders", properties=[Field(name="id")], identity=["id"]),
        ],
        blank_vertices=[],
        force_types={},
    )
    ec = EdgeConfig(edges=[Edge(source="users", target="orders", relation=None)])
    core = CoreSchema(vertex_config=vc, edge_config=ec)
    schema = Schema(metadata=meta, core_schema=core)
    ingestion = {
        "resources": [
            {
                "name": "users",
                "apply": pipeline_a or [{"vertex": "users"}],
            },
            {
                "name": "orders",
                "apply": pipeline_b or [{"vertex": "orders"}],
            },
        ],
        "transforms": [],
    }
    manifest = GraphManifest.from_config(
        {
            "schema": schema.to_dict(skip_defaults=False),
            "ingestion_model": ingestion,
        }
    )
    manifest.finish_init()
    return manifest


def _vertex_actor_step(resource_pipeline: list[dict]) -> dict:
    """Return the (single, expected) vertex step from a pipeline."""
    for step in resource_pipeline:
        if "vertex" in step:
            return step
    raise AssertionError(f"no vertex step in pipeline: {resource_pipeline}")


# -- RenameVertexFieldsOp ----------------------------------------------------


def test_rename_vertex_fields_injects_from_when_absent():
    """When VertexActor has no `from:`, rename injects `{new_field: old_field}`."""
    manifest = _build_manifest()

    apply_rename_vertex_fields(
        manifest,
        RenameVertexFieldsOp(renames={"users": {"user-name": "user_name"}}),
    )

    schema = manifest.require_schema()
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert "user_name" in user_props
    assert "user-name" not in user_props

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    step = _vertex_actor_step(pipeline)
    assert step["from"] == {"user_name": "user-name"}


def test_rename_vertex_fields_rewrites_existing_from_keys():
    """Existing `from: {old_field: doc_col}` becomes `{new_field: doc_col}`."""
    manifest = _build_manifest(
        pipeline_a=[{"vertex": "users", "from": {"user-name": "raw_name"}}]
    )

    apply_rename_vertex_fields(
        manifest,
        RenameVertexFieldsOp(renames={"users": {"user-name": "user_name"}}),
    )

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    step = _vertex_actor_step(pipeline)
    assert step["from"] == {"user_name": "raw_name"}


def test_rename_vertex_fields_rewrites_identity():
    manifest = _build_manifest(
        user_properties=[Field(name="user-id"), Field(name="user-name")],
        user_identity=["user-id"],
    )

    apply_rename_vertex_fields(
        manifest,
        RenameVertexFieldsOp(renames={"users": {"user-id": "user_id"}}),
    )

    schema = manifest.require_schema()
    users = schema.core_schema.vertex_config["users"]
    assert users.identity == ["user_id"]
    assert "user_id" in [f.name for f in users.properties]


def test_rename_vertex_fields_validates_unknown_vertex():
    manifest = _build_manifest()
    try:
        apply_rename_vertex_fields(
            manifest,
            RenameVertexFieldsOp(renames={"missing": {"x": "y"}}),
        )
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "missing" in str(exc)


# -- SanitizeOp end-to-end ---------------------------------------------------


def test_apply_sanitize_no_reserved_words_is_noop():
    """Without reserved words, sanitize should leave the manifest essentially intact."""
    manifest = _build_manifest()
    pipeline_before = manifest.require_ingestion_model().resources[0].pipeline

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.ARANGO))

    pipeline_after = manifest.require_ingestion_model().resources[0].pipeline
    assert pipeline_before == pipeline_after


def test_apply_sanitize_with_explicit_reserved_words_renames_field():
    """Explicit reserved word triggers vertex field rename + `from:` injection."""
    manifest = _build_manifest(
        user_properties=[Field(name="id"), Field(name="package")],
    )

    apply_sanitize(
        manifest,
        SanitizeOp(db_flavor=DBType.ARANGO, reserved_words=["package"]),
    )

    schema = manifest.require_schema()
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert "package" not in user_props
    assert "package_attr" in user_props

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    step = _vertex_actor_step(pipeline)
    assert step["from"] == {"package_attr": "package"}


def test_apply_evolution_dispatches_sanitize_op():
    """`apply_evolution` should dispatch `SanitizeOp` like other ops."""
    manifest = _build_manifest()

    out = apply_evolution(
        manifest,
        [SanitizeOp(db_flavor=DBType.ARANGO, reserved_words=["user-name"])],
        bump_version=False,
    )

    schema = out.require_schema()
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert "user-name_attr" in user_props


# -- regression: postgres-style flow with hyphenated columns -----------------


def test_sanitizer_propagates_hyphenated_field_rename_to_actors_for_tigergraph():
    """Regression for the bug fix: hyphenated columns must reach actors as `from:`.

    A vertex property whose name is *not* TigerGraph-reserved but whose name is
    illegal as an attribute identifier (hyphen) gets sanitized to an underscore
    form. The `VertexActor.from` must then reflect the rename so the doc, which
    still uses the hyphenated column, lands on the renamed property.
    """
    manifest = _build_manifest(
        user_properties=[
            Field(name="id"),
            # 'package' is a TigerGraph reserved word -> will be sanitized
            Field(name="package"),
        ],
    )

    Sanitizer(DBType.TIGERGRAPH).sanitize_manifest(manifest)

    schema = manifest.require_schema()
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert "package" not in user_props
    assert any(name.lower().startswith("package_attr") for name in user_props)

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    step = _vertex_actor_step(pipeline)
    assert step.get("from"), (
        "Sanitizer must inject `from:` covering the renamed field. "
        f"got pipeline={pipeline}"
    )
    assert "package" in step["from"].values()


def test_sanitize_preserves_merged_duplicate_vertex_fields():
    """Duplicate logical fields are merged before sanitize rewrite."""
    manifest = _build_manifest(
        user_properties=[
            Field(name="id"),
            Field(name="package"),
            Field(name="package", type=FieldType.STRING),
        ],
    )

    apply_sanitize(
        manifest,
        SanitizeOp(db_flavor=DBType.ARANGO, reserved_words=["package"]),
    )

    schema = manifest.require_schema()
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert user_props.count("package_attr") == 1
