"""Tests for PostgreSQL resource mapping."""

from collections import defaultdict

from graflo.architecture.edge import EdgeConfig
from graflo.architecture.onto_sql import (
    ColumnInfo,
    EdgeTableInfo,
    SchemaIntrospectionResult,
    VertexTableInfo,
)
from graflo.architecture.vertex import Field, Vertex, VertexConfig
from graflo.db.postgres.resource_mapping import PostgresResourceMapper
from graflo.hq.fuzzy_matcher import FuzzyMatcher


def _build_vertex_config() -> VertexConfig:
    return VertexConfig(
        vertices=[
            Vertex(
                name="users",
                fields=[Field(name="id"), Field(name="name"), Field(name="user_name")],
                identity=["id"],
            ),
            Vertex(
                name="products",
                fields=[
                    Field(name="product_code"),
                    Field(name="name"),
                    Field(name="product_name"),
                ],
                identity=["product_code"],
            ),
        ]
    )


def _build_edge_table_info() -> EdgeTableInfo:
    return EdgeTableInfo(
        name="purchases",
        schema_name="public",
        columns=[
            ColumnInfo(name="user_id", type="int4"),
            ColumnInfo(name="product_id", type="int4"),
            ColumnInfo(name="user-name", type="varchar"),
            ColumnInfo(name="product-name", type="varchar"),
            ColumnInfo(name="quantity", type="int4"),
        ],
        primary_key=["id"],
        foreign_keys=[],
        source_table="users",
        target_table="products",
        source_column="user_id",
        target_column="product_id",
        relation="purchases",
    )


def test_create_vertex_resource_with_field_mappings():
    mapper = PostgresResourceMapper()
    attribute_mappings = defaultdict(dict, {"users": {"user-name": "user_name"}})

    resource = mapper.create_vertex_resource(
        table_name="users",
        vertex_name="users",
        vertex_attribute_mappings=attribute_mappings,
    )

    assert resource.name == "users"
    assert resource.pipeline == [
        {"vertex": "users", "from": {"user_name": "user-name"}}
    ]


def test_create_vertex_resource_without_field_mappings():
    mapper = PostgresResourceMapper()
    attribute_mappings = defaultdict(dict)

    resource = mapper.create_vertex_resource(
        table_name="products",
        vertex_name="products",
        vertex_attribute_mappings=attribute_mappings,
    )

    assert resource.pipeline == [{"vertex": "products"}]


def test_create_edge_resource_uses_identity_and_sanitized_mappings():
    mapper = PostgresResourceMapper()
    edge_table = _build_edge_table_info()
    vertex_config = _build_vertex_config()
    matcher = FuzzyMatcher(["users", "products"], threshold=0.8, enable_cache=True)
    attribute_mappings = defaultdict(
        dict,
        {
            "users": {"user-name": "user_name"},
            "products": {"product-name": "product_name"},
        },
    )

    resource = mapper.create_edge_resource(
        edge_table_info=edge_table,
        vertex_config=vertex_config,
        matcher=matcher,
        vertex_attribute_mappings=attribute_mappings,
    )

    assert resource.name == "purchases"
    assert resource.pipeline == [
        {"vertex": "users", "from": {"id": "user_id", "user_name": "user-name"}},
        {
            "vertex": "products",
            "from": {"product_code": "product_id", "product_name": "product-name"},
        },
    ]


def test_create_edge_resource_raises_for_unknown_vertex():
    mapper = PostgresResourceMapper()
    edge_table = _build_edge_table_info().model_copy(
        update={"target_table": "missing_vertex"}
    )
    vertex_config = _build_vertex_config()
    matcher = FuzzyMatcher(["users", "products"], threshold=0.8, enable_cache=True)
    attribute_mappings = defaultdict(dict)

    try:
        mapper.create_edge_resource(
            edge_table_info=edge_table,
            vertex_config=vertex_config,
            matcher=matcher,
            vertex_attribute_mappings=attribute_mappings,
        )
        assert False, "Expected ValueError for unknown target vertex"
    except ValueError as exc:
        assert "Target vertex 'missing_vertex'" in str(exc)


def test_create_resources_from_tables_skips_invalid_edges():
    mapper = PostgresResourceMapper()
    vertex_config = _build_vertex_config()
    edge_table_valid = _build_edge_table_info()
    edge_table_invalid = edge_table_valid.model_copy(
        update={"name": "bad_rel", "target_table": "missing_vertex"}
    )
    introspection_result = SchemaIntrospectionResult(
        schema_name="public",
        vertex_tables=[
            # only name is relevant for mapper vertex resources
            # other fields are intentionally minimal but valid
            VertexTableInfo(
                name="users",
                schema_name="public",
                columns=[],
                primary_key=["id"],
                foreign_keys=[],
            ),
            VertexTableInfo(
                name="products",
                schema_name="public",
                columns=[],
                primary_key=["product_code"],
                foreign_keys=[],
            ),
        ],
        edge_tables=[edge_table_valid, edge_table_invalid],
        raw_tables=[],
    )
    attribute_mappings = defaultdict(
        dict,
        {
            "users": {"user-name": "user_name"},
            "products": {"product-name": "product_name"},
        },
    )

    resources = mapper.create_resources_from_tables(
        introspection_result=introspection_result,
        vertex_config=vertex_config,
        edge_config=EdgeConfig(),
        vertex_attribute_mappings=attribute_mappings,
        fuzzy_threshold=0.8,
    )

    resource_names = [resource.name for resource in resources]
    assert resource_names == ["users", "products", "purchases"]
