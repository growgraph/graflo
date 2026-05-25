"""Tests for PostgreSQL resource mapping.

These tests cover the *un-sanitized* mapping output (resources use original
PostgreSQL column names). Reserved-word / DB-flavor renames are applied a
posteriori via :class:`graflo.hq.sanitizer.Sanitizer` and exercised in
``test/architecture/evolution/test_sanitize.py``.
"""

from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.onto_sql import (
    ColumnInfo,
    EdgeTableInfo,
    SchemaIntrospectionResult,
    VertexTableInfo,
)
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.postgres.resource_mapping import PostgresResourceMapper
from graflo.hq.fuzzy_matcher import FuzzyMatcher


def _build_vertex_config() -> VertexConfig:
    return VertexConfig(
        vertices=[
            Vertex(
                name="users",
                properties=[
                    Field(name="id"),
                    Field(name="name"),
                    Field(name="user_name"),
                ],
                identity=["id"],
            ),
            Vertex(
                name="products",
                properties=[
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


def test_create_vertex_resource_emits_minimal_pipeline():
    mapper = PostgresResourceMapper()

    resource = mapper.create_vertex_resource(
        table_name="users",
        vertex_name="users",
    )

    assert resource.name == "users"
    assert resource.pipeline == [{"vertex": "users"}]


def test_create_edge_resource_uses_identity_columns():
    mapper = PostgresResourceMapper()
    edge_table = _build_edge_table_info()
    vertex_config = _build_vertex_config()
    matcher = FuzzyMatcher(["users", "products"], threshold=0.8, enable_cache=True)

    resource = mapper.create_edge_resource(
        edge_table_info=edge_table,
        vertex_config=vertex_config,
        matcher=matcher,
    )

    assert resource.name == "purchases"
    assert resource.pipeline == [
        {"vertex": "users", "from": {"id": "user_id"}},
        {"vertex": "products", "from": {"product_code": "product_id"}},
    ]


def test_create_edge_resource_raises_for_unknown_vertex():
    mapper = PostgresResourceMapper()
    edge_table = _build_edge_table_info().model_copy(
        update={"target_table": "missing_vertex"}
    )
    vertex_config = _build_vertex_config()
    matcher = FuzzyMatcher(["users", "products"], threshold=0.8, enable_cache=True)

    try:
        mapper.create_edge_resource(
            edge_table_info=edge_table,
            vertex_config=vertex_config,
            matcher=matcher,
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

    resources = mapper.create_resources_from_tables(
        introspection_result=introspection_result,
        vertex_config=vertex_config,
        edge_config=EdgeConfig(),
        fuzzy_threshold=0.8,
    )

    resource_names = [resource.name for resource in resources]
    assert resource_names == ["users", "products", "purchases"]
