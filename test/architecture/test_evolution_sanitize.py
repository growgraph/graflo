"""Tests for :mod:`graflo.architecture.evolution` sanitize / rename ops."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    RenameVertexPropertiesOp,
    SanitizeOp,
    apply_evolution,
    apply_rename_vertex_properties,
    apply_sanitize,
)
from graflo.architecture.graph_types import Index
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.hq.sanitizer import Sanitizer
from graflo.onto import DBType


def _build_manifest(
    *,
    pipeline_a: list[dict] | None = None,
    pipeline_b: list[dict] | None = None,
    user_properties: list[Field] | None = None,
    user_identity: list[str] | None = None,
    extra_weights_users: list[dict] | None = None,
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
        force_types={},
    )
    ec = EdgeConfig(edges=[Edge(source="users", target="orders", relation=None)])
    core = CoreSchema(vertex_config=vc, edge_config=ec)
    schema = Schema(metadata=meta, core_schema=core)
    users_resource: dict = {
        "name": "users",
        "pipeline": pipeline_a or [{"vertex": "users"}],
    }
    if extra_weights_users is not None:
        users_resource["extra_weights"] = extra_weights_users
    ingestion = {
        "resources": [
            users_resource,
            {
                "name": "orders",
                "pipeline": pipeline_b or [{"vertex": "orders"}],
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


# -- RenameVertexPropertiesOp ----------------------------------------------------


def test_rename_vertex_fields_injects_from_when_absent():
    """When VertexActor has no `from:`, rename injects `{new_field: old_field}`."""
    manifest = _build_manifest()

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-name": "user_name"}}),
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

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-name": "user_name"}}),
    )

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    step = _vertex_actor_step(pipeline)
    assert step["from"] == {"user_name": "raw_name"}


def test_rename_vertex_fields_rewrites_identity():
    manifest = _build_manifest(
        user_properties=[Field(name="user-id"), Field(name="user-name")],
        user_identity=["user-id"],
    )

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-id": "user_id"}}),
    )

    schema = manifest.require_schema()
    users = schema.core_schema.vertex_config["users"]
    assert users.identity == ["user_id"]
    assert "user_id" in [f.name for f in users.properties]


def test_rename_vertex_fields_validates_unknown_vertex():
    manifest = _build_manifest()
    try:
        apply_rename_vertex_properties(
            manifest,
            RenameVertexPropertiesOp(renames={"missing": {"x": "y"}}),
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

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"id": "user_id"}}),
    )

    schema = manifest.require_schema()
    users = schema.core_schema.vertex_config["users"]
    by_name = {field.name: field for field in users.properties}

    assert users.identity == ["user_id"]
    assert "id" not in by_name
    assert by_name["user_id"].type == FieldType.STRING


def test_rename_vertex_fields_refuses_a_fold_nobody_wrote_down():
    """``{a: b}`` onto a declared ``b`` would turn the key ``[a, b]`` into ``[b]``."""
    manifest = _build_manifest(
        user_properties=[Field(name="a"), Field(name="b")],
        user_identity=["a", "b"],
    )

    with pytest.raises(ValueError, match="fold two properties into one"):
        apply_rename_vertex_properties(
            manifest,
            RenameVertexPropertiesOp(renames={"users": {"a": "b"}}),
        )


def test_rename_vertex_fields_dedupes_identity_when_names_collide():
    """A fold the map spells out is applied, and leaves no duplicate names."""
    manifest = _build_manifest(
        user_properties=[Field(name="a"), Field(name="b")],
        user_identity=["a", "b"],
    )

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"a": "b", "b": "b"}}),
    )

    schema = manifest.require_schema()
    users = schema.core_schema.vertex_config["users"]
    assert users.identity == ["b"]
    assert [field.name for field in users.properties].count("b") == 1


def test_rename_vertex_fields_preserves_call_transform_without_rename():
    """``transform.call`` must not gain a synthesized ``rename`` map."""
    manifest = _build_manifest(
        pipeline_a=[
            {
                "type": "transform",
                "call": {
                    "module": "builtins",
                    "foo": "str",
                    "input": ["id"],
                    "output": ["aux"],
                },
            },
            {"type": "vertex", "vertex": "users"},
        ],
    )

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-name": "user_name"}}),
    )

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    tf = pipeline[0]
    assert tf.get("type") == "transform"
    assert "rename" not in tf
    assert "call" in tf


def test_rename_vertex_fields_no_spurious_rename_map_entries():
    """Do not inject rename entries for unrelated in-scope vertex renames."""
    manifest = _build_manifest(
        pipeline_a=[
            {"type": "vertex", "vertex": "users"},
            {"type": "transform", "rename": {"other_col": "other_field"}},
        ],
    )

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-name": "user_name"}}),
    )

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    assert pipeline[1]["rename"] == {"other_col": "other_field"}


def test_rename_vertex_fields_nested_rename_not_augmented_by_parent_scope():
    """Nested pipelines must not inherit spurious rename keys from ancestor scope."""
    manifest = _build_manifest(
        pipeline_a=[
            {"type": "vertex", "vertex": "users"},
            {
                "type": "descend",
                "key": "nested",
                "pipeline": [
                    {"type": "vertex", "vertex": "orders"},
                    {"type": "transform", "rename": {"amt": "amount"}},
                ],
            },
        ],
    )

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-name": "user_name"}}),
    )

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    descend = pipeline[1]
    assert descend["type"] == "descend"
    inner = descend["pipeline"]
    assert inner[1]["rename"] == {"amt": "amount"}


def test_rename_vertex_fields_updates_extra_weights_vertex_weights():
    manifest = _build_manifest(
        extra_weights_users=[
            {
                "edge": {"source": "users", "target": "orders"},
                "vertex_weights": [
                    {
                        "name": "users",
                        "fields": ["user-name"],
                        "filter": {"user-name": "keep"},
                        "map": {"user-name": "edge_col"},
                    }
                ],
            }
        ]
    )

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-name": "user_name"}}),
    )

    w = (
        manifest.require_ingestion_model()
        .resources[0]
        .extra_weights[0]
        .vertex_weights[0]
    )
    assert w.fields == ["user_name"]
    assert dict(w.filter) == {"user_name": "keep"}
    assert dict(w.map) == {"user_name": "edge_col"}


def test_rename_vertex_fields_updates_edge_actor_vertex_weights_in_pipeline():
    manifest = _build_manifest(
        pipeline_a=[
            {"type": "vertex", "vertex": "users"},
            {
                "type": "edge",
                "from": "users",
                "to": "orders",
                "vertex_weights": [
                    {"name": "users", "fields": ["user-name"]},
                ],
            },
        ],
    )

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"user-name": "user_name"}}),
    )

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    edge_step = pipeline[1]
    vw = edge_step["vertex_weights"][0]
    # The edge attribute belongs to the edge: it keeps its name, read from the
    # renamed vertex field.
    assert vw["fields"] == []
    assert vw["map"] == {"user_name": "users@user-name"}


def test_rename_vertex_fields_leaves_edge_indexes_alone():
    """An edge index names edge attributes, which a vertex rename does not rename."""
    from graflo.architecture.schema.database_features import EdgePhysicalSpec

    manifest = _build_manifest(
        user_properties=[Field(name="id"), Field(name="since")],
    )
    schema = manifest.require_schema()
    schema.core_schema.edge_config.edges[0].properties = [Field(name="since")]
    schema.db_profile.edge_specs = [
        EdgePhysicalSpec(
            source="users", target="orders", indexes=[Index(fields=["since"])]
        )
    ]
    schema.finish_init()

    apply_rename_vertex_properties(
        manifest,
        RenameVertexPropertiesOp(renames={"users": {"since": "joined"}}),
    )

    spec = manifest.require_schema().db_profile.edge_specs[0]
    assert spec.indexes[0].fields == ["since"]


# -- SanitizeOp end-to-end ---------------------------------------------------


def test_apply_sanitize_no_reserved_words_is_noop():
    """Without reserved words, sanitize should leave the manifest essentially intact."""
    manifest = _build_manifest()
    pipeline_before = manifest.require_ingestion_model().resources[0].pipeline

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.ARANGO))

    pipeline_after = manifest.require_ingestion_model().resources[0].pipeline
    assert pipeline_before == pipeline_after


def test_apply_sanitize_with_explicit_reserved_words_records_physical_name():
    """A reserved property gets a stored name in the profile, and only there."""
    manifest = _build_manifest(
        user_properties=[Field(name="id"), Field(name="package")],
    )
    pipeline_before = manifest.require_ingestion_model().resources[0].pipeline

    apply_sanitize(
        manifest,
        SanitizeOp(db_flavor=DBType.ARANGO, reserved_words=["package"]),
    )

    schema = manifest.require_schema()
    assert schema.db_profile.vertex_property_names == {
        "users": {"package": "package_attr"}
    }
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert user_props == ["id", "package"]
    pipeline_after = manifest.require_ingestion_model().resources[0].pipeline
    assert pipeline_after == pipeline_before


def test_apply_sanitize_preserves_call_transform_without_rename():
    """Reserved-word field rename must not add ``rename`` to ``transform.call`` steps."""
    manifest = _build_manifest(
        user_properties=[Field(name="id"), Field(name="package")],
        pipeline_a=[
            {
                "type": "transform",
                "call": {
                    "module": "builtins",
                    "foo": "str",
                    "input": ["id"],
                    "output": ["sink"],
                },
            },
            {"type": "vertex", "vertex": "users"},
        ],
    )

    apply_sanitize(
        manifest,
        SanitizeOp(db_flavor=DBType.ARANGO, reserved_words=["package"]),
    )

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    tf = pipeline[0]
    assert tf.get("type") == "transform"
    assert "rename" not in tf
    assert "call" in tf


def test_apply_evolution_dispatches_sanitize_op():
    """`apply_evolution` should dispatch `SanitizeOp` like other ops."""
    manifest = _build_manifest()

    out = apply_evolution(
        manifest,
        [SanitizeOp(db_flavor=DBType.ARANGO, reserved_words=["user-name"])],
        bump_version=False,
    )

    schema = out.require_schema()
    assert schema.db_profile.vertex_property_name("users", "user-name") == (
        "user-name_attr"
    )
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert "user-name" in user_props


# -- regression: postgres-style flow with hyphenated columns -----------------


def test_sanitizer_leaves_logical_names_and_pipeline_alone_for_tigergraph():
    """A name TigerGraph cannot store is a physical fact: the logical model keeps it.

    Documents still arrive keyed ``package``; the writer maps them onto the
    stored attribute, so no ``from:`` has to be injected into the pipeline.
    """
    manifest = _build_manifest(
        user_properties=[
            Field(name="id"),
            # 'package' is a TigerGraph reserved word
            Field(name="package"),
        ],
    )
    pipeline_before = manifest.require_ingestion_model().resources[0].pipeline

    Sanitizer(DBType.TIGERGRAPH).sanitize_manifest(manifest)

    schema = manifest.require_schema()
    assert schema.db_profile.db_flavor == DBType.TIGERGRAPH
    assert schema.db_profile.vertex_property_name("users", "package") == (
        "package_attr"
    )
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert "package" in user_props
    assert manifest.require_ingestion_model().resources[0].pipeline == pipeline_before


def test_sanitize_preserves_merged_duplicate_vertex_fields():
    """Duplicate logical fields are merged, then given one stored name."""
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
    assert user_props.count("package") == 1
    assert schema.db_profile.vertex_property_map("users") == {"package": "package_attr"}


# -- relations whose endpoints have different identities ---------------------


def _build_multi_relation_manifest(
    *,
    vertex_a_properties: list[Field] | None = None,
    vertex_a_identity: list[str] | None = None,
    vertex_indexes: dict | None = None,
) -> GraphManifest:
    """Manifest with three source vertices sharing the same relation 'owns'.

    UserA's identity (``source_id``) differs from UserB's and UserC's (``id``).

    Vertex layout:
        UserA --(owns)--> Target
        UserB --(owns)--> Target
        UserC --(owns)--> Target
    """
    from graflo.architecture.schema.database_features import DatabaseProfile

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
            {"name": "UserA", "pipeline": [{"vertex": "UserA"}]},
            {"name": "UserB", "pipeline": [{"vertex": "UserB"}]},
            {"name": "UserC", "pipeline": [{"vertex": "UserC"}]},
            {"name": "Target", "pipeline": [{"vertex": "Target"}]},
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


def test_sanitize_leaves_identities_alone_across_a_shared_relation():
    """Vertices on one relation keep their own identities.

    TigerGraph edge DDL (``FROM A, TO B``) names no identity field, and the writer
    passes each edge's own source and target keys, so nothing needs a relation's
    endpoints to share identity names. Renaming them was also lossy: identities of
    different lengths gained untyped fields no document fills.
    """
    manifest = _build_multi_relation_manifest(
        vertex_indexes={"UserA": [Index(fields=["source_id"])]},
    )

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    user_a = schema.core_schema.vertex_config["UserA"]
    assert user_a.identity == ["source_id"]
    assert {f.name: f.type for f in user_a.properties} == {"source_id": FieldType.INT}
    assert schema.db_profile.vertex_indexes["UserA"][0].fields == ["source_id"]
    assert schema.db_profile.vertex_property_names == {}


# -- TigerGraph identifier sanitization (invalid chars, prefixes, reserved) --


def test_sanitize_tigergraph_identifier_uses_double_underscore_replacement():
    from graflo.db.util import (
        TIGERGRAPH_INVALID_CHAR_REPLACEMENT,
        load_tigergraph_identifier_rules,
        sanitize_tigergraph_identifier,
    )

    rules = load_tigergraph_identifier_rules()
    assert rules is not None
    reserved = set(rules.reserved_words_upper)

    assert TIGERGRAPH_INVALID_CHAR_REPLACEMENT == "__"
    assert (
        sanitize_tigergraph_identifier(
            "my-entity",
            reserved,
            rules.forbidden_prefixes,
            rules.invalid_characters,
        )
        == "my__entity"
    )
    assert (
        sanitize_tigergraph_identifier(
            "user_name",
            reserved,
            rules.forbidden_prefixes,
            rules.invalid_characters,
        )
        == "user_name"
    )
    assert (
        sanitize_tigergraph_identifier(
            "a.b",
            reserved,
            rules.forbidden_prefixes,
            rules.invalid_characters,
        )
        == "a__b"
    )
    # A DDL reserved word reaches the sanitizer, not only the DDL validator.
    assert (
        sanitize_tigergraph_identifier(
            "file",
            reserved,
            rules.forbidden_prefixes,
            rules.invalid_characters,
        )
        == "file_attr"
    )


def _build_tigergraph_manifest(
    *,
    vertices: list[Vertex] | None = None,
    edges: list[Edge] | None = None,
    pipeline_a: list[dict] | None = None,
    resource_name: str | None = None,
) -> GraphManifest:
    from graflo.architecture.schema.database_features import DatabaseProfile

    meta = GraphMetadata(name="tg_sanitize", version="1.0.0")
    vertex_list = vertices or [
        Vertex(
            name="my-entity",
            properties=[Field(name="id")],
            identity=["id"],
        ),
    ]
    vc = VertexConfig(
        vertices=vertex_list,
        force_types={},
    )
    default_edges = [
        Edge(
            source="my-entity",
            target="my-entity",
            relation="knows-someone",
        ),
    ]
    ec = EdgeConfig(edges=default_edges if edges is None else edges)
    core = CoreSchema(vertex_config=vc, edge_config=ec)
    schema = Schema(
        metadata=meta,
        core_schema=core,
        db_profile=DatabaseProfile(db_flavor=DBType.TIGERGRAPH),
    )
    primary_vertex = resource_name or vertex_list[0].name
    ingestion = {
        "resources": [
            {
                "name": primary_vertex,
                "pipeline": pipeline_a or [{"vertex": primary_vertex}],
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


def test_apply_sanitize_tigergraph_vertex_storage_name_invalid_chars():
    manifest = _build_tigergraph_manifest()
    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    assert schema.db_profile.vertex_storage_name("my-entity") == "my__entity"


def test_apply_sanitize_tigergraph_edge_relation_name_invalid_chars():
    manifest = _build_tigergraph_manifest()
    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    edge = schema.core_schema.edge_config.edges[0]
    assert (
        schema.db_profile.edge_relation_name(
            edge.edge_id,
            default_relation=edge.relation,
        )
        == "knows__someone"
    )


def test_apply_sanitize_tigergraph_field_invalid_chars_records_stored_name():
    manifest = _build_tigergraph_manifest(
        vertices=[
            Vertex(
                name="users",
                properties=[Field(name="id"), Field(name="user-name")],
                identity=["id"],
            ),
        ],
        edges=[],
        pipeline_a=[{"vertex": "users"}],
        resource_name="users",
    )
    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    assert schema.db_profile.vertex_property_map("users") == {"user-name": "user__name"}
    user_props = [f.name for f in schema.core_schema.vertex_config["users"].properties]
    assert user_props == ["id", "user-name"]

    pipeline = manifest.require_ingestion_model().resources[0].pipeline
    step = _vertex_actor_step(pipeline)
    assert "from" not in step


def test_apply_sanitize_tigergraph_forbidden_prefix():
    manifest = _build_tigergraph_manifest(
        vertices=[
            Vertex(
                name="gsql_sys_foo",
                properties=[Field(name="id")],
                identity=["id"],
            ),
        ],
        edges=[],
    )
    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    assert schema.db_profile.vertex_storage_name("gsql_sys_foo") == "tg_gsql_sys_foo"


def test_apply_sanitize_tigergraph_reserved_word_vertex_storage_name():
    manifest = _build_tigergraph_manifest(
        vertices=[
            Vertex(
                name="package",
                properties=[Field(name="id")],
                identity=["id"],
            ),
        ],
        edges=[],
    )
    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    schema = manifest.require_schema()
    assert schema.db_profile.vertex_storage_name("package") == "package_vertex"
