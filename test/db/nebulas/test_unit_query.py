"""Unit tests for NebulaGraph nGQL query builders (no Docker required)."""

from graflo.architecture.vertex import Field, FieldType
from graflo.db.nebula.query import (
    aggregate_ngql,
    aggregate_gql,
    batch_upsert_vertices_ngql,
    create_edge_type_ngql,
    create_edge_index_ngql,
    create_space_ngql,
    create_tag_index_ngql,
    create_tag_ngql,
    drop_space_ngql,
    fetch_docs_ngql,
    fetch_docs_gql,
    fetch_edges_ngql,
    insert_edges_ngql,
    insert_vertices_ngql,
    upsert_vertex_gql,
)


# ── DDL: space management ────────────────────────────────────────────────


def test_create_space():
    q = create_space_ngql("myspace")
    assert "CREATE SPACE IF NOT EXISTS `myspace`" in q
    assert "vid_type=FIXED_STRING(256)" in q


def test_create_space_custom_vid():
    q = create_space_ngql("s", vid_type="INT64", partition_num=10, replica_factor=3)
    assert "vid_type=INT64" in q
    assert "partition_num=10" in q
    assert "replica_factor=3" in q


def test_drop_space():
    assert drop_space_ngql("s") == "DROP SPACE IF EXISTS `s`"


# ── DDL: tag / edge type creation ────────────────────────────────────────


def test_create_tag():
    fields = [
        Field(name="name", type=FieldType.STRING),
        Field(name="age", type=FieldType.INT),
    ]
    q = create_tag_ngql("Person", fields)
    assert "CREATE TAG IF NOT EXISTS `Person`" in q
    assert "`name` string" in q
    assert "`age` int64" in q


def test_create_tag_no_fields():
    q = create_tag_ngql("Empty", [])
    assert "CREATE TAG IF NOT EXISTS `Empty` ()" in q


def test_create_edge_type():
    q = create_edge_type_ngql("follows")
    assert "CREATE EDGE IF NOT EXISTS `follows`" in q


def test_create_edge_type_with_fields():
    fields = [Field(name="weight", type=FieldType.DOUBLE)]
    q = create_edge_type_ngql("follows", fields)
    assert "`weight` double" in q


# ── DDL: index creation ──────────────────────────────────────────────────


def test_create_tag_index():
    q = create_tag_index_ngql("idx_p_name", "Person", ["name"])
    assert "CREATE TAG INDEX IF NOT EXISTS `idx_p_name`" in q
    assert "ON `Person`" in q


def test_create_tag_index_non_string_field():
    q = create_tag_index_ngql("idx_p_age", "Person", ["age"], string_fields={"name"})
    assert "`age`" in q
    assert "(`age`)" not in q or "(`age`(" not in q


def test_create_edge_index():
    q = create_edge_index_ngql("idx_e_w", "follows", ["weight"])
    assert "CREATE EDGE INDEX IF NOT EXISTS `idx_e_w`" in q
    assert "ON `follows`" in q


# ── DML: vertex operations ───────────────────────────────────────────────


def test_batch_upsert_vertices():
    docs = [{"name": "Alice", "age": 30}]
    stmts = batch_upsert_vertices_ngql("Person", docs, ["name"], ["name", "age"])
    assert len(stmts) == 1
    assert "UPSERT VERTEX ON `Person`" in stmts[0]
    assert '"Alice"' in stmts[0]


def test_batch_upsert_vertices_multiple():
    docs = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]
    stmts = batch_upsert_vertices_ngql("Person", docs, ["name"], ["name", "age"])
    assert len(stmts) == 2


def test_insert_vertices():
    docs = [{"name": "Alice", "age": 30}]
    q = insert_vertices_ngql("Person", docs, ["name"], ["name", "age"])
    assert "INSERT VERTEX IF NOT EXISTS `Person`" in q
    assert '"Alice"' in q


def test_insert_vertices_empty():
    q = insert_vertices_ngql("Person", [], ["name"], ["name", "age"])
    assert q == ""


def test_upsert_vertex_gql_delegates():
    from graflo.db.nebula.query import upsert_vertex_ngql

    q_ngql = upsert_vertex_ngql("T", "v1", {"x": 1}, ["x"])
    q_gql = upsert_vertex_gql("T", "v1", {"x": 1}, ["x"])
    assert q_ngql == q_gql


# ── DML: edge operations ─────────────────────────────────────────────────


def test_insert_edges():
    edges = [("Alice", "Berlin", {"since": 2020})]
    q = insert_edges_ngql("lives_in", edges, ["since"])
    assert "INSERT EDGE IF NOT EXISTS `lives_in`" in q
    assert '"Alice"->"Berlin"' in q


def test_insert_edges_no_props():
    edges = [("Alice", "Berlin", {})]
    q = insert_edges_ngql("lives_in", edges)
    assert "INSERT EDGE IF NOT EXISTS `lives_in`" in q
    assert '"Alice"->"Berlin"' in q


def test_insert_edges_empty():
    q = insert_edges_ngql("lives_in", [])
    assert q == ""


# ── DQL: fetch queries (nGQL) ────────────────────────────────────────────


def test_fetch_docs_ngql_basic():
    q = fetch_docs_ngql("Person")
    assert "MATCH (v:`Person`)" in q
    assert "RETURN v" in q


def test_fetch_docs_ngql_with_limit():
    q = fetch_docs_ngql("Person", limit=10)
    assert "LIMIT 10" in q


def test_fetch_docs_ngql_with_filter():
    q = fetch_docs_ngql("Person", filter_clause="v.`Person`.`age` > 25")
    assert "WHERE v.`Person`.`age` > 25" in q


def test_fetch_docs_ngql_with_return_keys():
    q = fetch_docs_ngql("Person", return_keys=["name", "age"])
    assert "v.`Person`.`name` AS `name`" in q
    assert "v.`Person`.`age` AS `age`" in q


def test_fetch_edges_ngql():
    q = fetch_edges_ngql("Person", "Alice", edge_type="lives_in")
    assert 'GO FROM "Alice"' in q
    assert "OVER `lives_in`" in q


def test_fetch_edges_ngql_with_target():
    q = fetch_edges_ngql("Person", "Alice", edge_type="lives_in", to_vid="Berlin")
    assert '"Berlin"' in q


# ── DQL: fetch queries (GQL v5) ──────────────────────────────────────────


def test_fetch_docs_gql_basic():
    q = fetch_docs_gql("Person")
    assert "MATCH (v:`Person`)" in q
    assert "RETURN v" in q


def test_fetch_docs_gql_with_return_keys():
    q = fetch_docs_gql("Person", return_keys=["name"])
    assert "v.`name` AS `name`" in q


# ── DQL: aggregation (nGQL) ──────────────────────────────────────────────


def test_aggregate_count():
    q = aggregate_ngql("Person", "COUNT")
    assert "count(*)" in q


def test_aggregate_count_with_discriminant():
    q = aggregate_ngql("Person", "COUNT", discriminant="age")
    assert "v.`Person`.`age`" in q
    assert "count(*)" in q


def test_aggregate_max():
    q = aggregate_ngql("Person", "MAX", aggregated_field="age")
    assert "max(v.`Person`.`age`)" in q


def test_aggregate_avg():
    q = aggregate_ngql("Person", "AVG", aggregated_field="age")
    assert "avg(v.`Person`.`age`)" in q


def test_aggregate_sorted_unique():
    q = aggregate_ngql("Person", "SORTED_UNIQUE", aggregated_field="name")
    assert "DISTINCT" in q
    assert "ORDER BY" in q


# ── DQL: aggregation (GQL v5) ────────────────────────────────────────────


def test_aggregate_gql_count():
    q = aggregate_gql("Person", "COUNT")
    assert "count(*)" in q


def test_aggregate_gql_max():
    q = aggregate_gql("Person", "MAX", aggregated_field="age")
    assert "max(v.`age`)" in q


def test_aggregate_gql_count_discriminant():
    q = aggregate_gql("Person", "COUNT", discriminant="age")
    assert "v.`age`" in q
