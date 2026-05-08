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


def test_rename_vertex_fields_does_not_add_old_identity_as_type_none():
    """Renaming an identity field should not reintroduce the old name as a ghost."""
    manifest = _build_manifest(
        user_properties=[Field(name="id", type=FieldType.STRING), Field(name="name")],
        user_identity=["id"],
    )

    apply_rename_vertex_fields(
        manifest,
        RenameVertexFieldsOp(renames={"users": {"id": "user_id"}}),
    )

    schema = manifest.require_schema()
    users = schema.core_schema.vertex_config["users"]
    by_name = {field.name: field for field in users.properties}

    assert users.identity == ["user_id"]
    assert "id" not in by_name
    assert by_name["user_id"].type == FieldType.STRING


def test_rename_vertex_fields_dedupes_identity_when_names_collide():
    """Identity rewrite should not keep duplicate names after a collision."""
    manifest = _build_manifest(
        user_properties=[Field(name="a"), Field(name="b")],
        user_identity=["a", "b"],
    )

    apply_rename_vertex_fields(
        manifest,
        RenameVertexFieldsOp(renames={"users": {"a": "b"}}),
    )

    schema = manifest.require_schema()
    users = schema.core_schema.vertex_config["users"]
    assert users.identity == ["b"]
    assert [field.name for field in users.properties].count("b") == 1


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


# -- normalize_relation_identity (TigerGraph identity normalization) ----------


def _build_multi_relation_manifest(
    *,
    vertex_a_properties: list[Field] | None = None,
    vertex_a_identity: list[str] | None = None,
    vertex_indexes: dict | None = None,
) -> GraphManifest:
    """Manifest with three source vertices sharing the same relation 'owns'.

    UserA has a diverging identity that will be normalized to match the
    majority identity used by UserB and UserC (["id"]).

    Vertex layout:
        UserA --(owns)--> Target
        UserB --(owns)--> Target
        UserC --(owns)--> Target
    """
    from graflo.architecture.database_features import DatabaseProfile

    meta = GraphMetadata(name="tg_normalize", version="1.0.0")

    a_props = vertex_a_properties or [Field(name="source_id", type=FieldType.INT)]
    a_identity = vertex_a_identity or ["source_id"]

    vc = VertexConfig(
        vertices=[
            Vertex(name="UserA", properties=a_props, identity=a_identity),
            Vertex(
                name="UserB",
                properties=[Field(name="id", type=FieldType.STRING)],
                identity=["id"],
            ),
            Vertex(
                name="UserC",
                properties=[Field(name="id", type=FieldType.STRING)],
                identity=["id"],
            ),
            Vertex(
                name="Target",
                properties=[Field(name="tid", type=FieldType.INT)],
                identity=["tid"],
            ),
        ],
        blank_vertices=[],
        force_types={},
    )
    ec = EdgeConfig(
        edges=[
            Edge(source="UserA", target="Target", relation="owns"),
            Edge(source="UserB", target="Target", relation="owns"),
            Edge(source="UserC", target="Target", relation="owns"),
        ]
    )
    db_profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        vertex_indexes=vertex_indexes or {},
    )
    core = CoreSchema(vertex_config=vc, edge_config=ec)
    schema = Schema(metadata=meta, core_schema=core, db_profile=db_profile)
    ingestion = {
        "resources": [
            {"name": "UserA", "apply": [{"vertex": "UserA"}]},
            {"name": "UserB", "apply": [{"vertex": "UserB"}]},
            {"name": "UserC", "apply": [{"vertex": "UserC"}]},
            {"name": "Target", "apply": [{"vertex": "Target"}]},
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


def test_identity_normalization_preserves_field_type():
    """Renamed identity field keeps its original type after normalization.

    UserA.source_id (INT) is renamed to 'id' to match the majority identity.
    The resulting field must still carry type=INT, not the default None.
    """
    manifest = _build_multi_relation_manifest()

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    user_a = schema.core_schema.vertex_config["UserA"]
    props_by_name = {f.name: f for f in user_a.properties}

    assert "source_id" not in props_by_name, "old field name must be removed"
    assert "id" in props_by_name, "new field name must be present"
    assert props_by_name["id"].type == FieldType.INT, (
        "type must be carried over from source_id (INT), not silently set to None"
    )
    assert user_a.identity == ["id"]


def test_identity_normalization_no_stale_field_on_overlap():
    """When old and new identity sets overlap, no stale field is left behind.

    UserA identity ("a", "b") is normalized to ("b", "c").
    per_vertex = {"a": "b", "b": "c"}.  The old set-membership removal logic
    kept "b" because it appears in new_fields; the rename-walk approach must
    remove it by renaming it to "c".
    """
    manifest = _build_multi_relation_manifest(
        vertex_a_properties=[
            Field(name="a", type=FieldType.INT),
            Field(name="b", type=FieldType.STRING),
            Field(name="extra"),
        ],
        vertex_a_identity=["a", "b"],
    )
    # Override UserB/UserC to use identity ("b", "c") so most_popular = ("b","c")
    schema = manifest.require_schema()
    vc = schema.core_schema.vertex_config
    for vname in ("UserB", "UserC"):
        v = vc[vname]
        if "c" not in v.property_names:
            v.properties.append(Field(name="c", type=FieldType.INT))
        v.identity = ["b", "c"]

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    user_a = schema.core_schema.vertex_config["UserA"]
    prop_names = user_a.property_names

    # "a" renamed to "b", "b" renamed to "c" — only the post-rename names remain
    assert "a" not in prop_names, "'a' must be gone after rename to 'b'"
    assert prop_names.count("b") == 1, "'b' must appear exactly once"
    assert "c" in prop_names, "'c' must be present after rename from 'b'"
    assert user_a.identity == ["b", "c"]

    # Types must be preserved: original "a"(INT)→"b", original "b"(STRING)→"c"
    props_by_name = {f.name: f for f in user_a.properties}
    assert props_by_name["b"].type == FieldType.INT
    assert props_by_name["c"].type == FieldType.STRING


def test_identity_normalization_updates_db_profile_vertex_indexes():
    """vertex_indexes referencing the renamed field are rewritten in DatabaseProfile."""
    from graflo.architecture.graph_types import Index

    manifest = _build_multi_relation_manifest(
        vertex_indexes={
            "UserA": [Index(fields=["source_id", "extra_field"])],
        },
    )

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    indexes = schema.db_profile.vertex_indexes.get("UserA", [])
    assert indexes, "UserA vertex_indexes must still exist"
    assert indexes[0].fields == ["id", "extra_field"], (
        "index field 'source_id' must be rewritten to 'id'; 'extra_field' unchanged"
    )


def test_identity_normalization_leaves_non_identity_properties_untouched():
    """Properties not involved in identity normalization keep name and type."""
    manifest = _build_multi_relation_manifest(
        vertex_a_properties=[
            Field(name="source_id", type=FieldType.INT),
            Field(name="email", type=FieldType.STRING),
            Field(name="score", type=FieldType.FLOAT),
        ],
    )

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    user_a = schema.core_schema.vertex_config["UserA"]
    props_by_name = {f.name: f for f in user_a.properties}

    assert "email" in props_by_name
    assert props_by_name["email"].type == FieldType.STRING
    assert "score" in props_by_name
    assert props_by_name["score"].type == FieldType.FLOAT
