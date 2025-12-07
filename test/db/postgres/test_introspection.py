"""Tests for PostgreSQL schema introspection functionality."""

import logging


logger = logging.getLogger(__name__)


def test_get_tables(postgres_conn, load_mock_schema):
    """Test getting all tables from the schema."""
    _ = load_mock_schema  # Ensure schema is loaded

    tables = postgres_conn.get_tables()

    # Should have 4 tables: users, products, purchases, follows
    table_names = {t["table_name"] for t in tables}
    assert "users" in table_names
    assert "products" in table_names
    assert "purchases" in table_names
    assert "follows" in table_names
    assert len(tables) == 4

    # Check schema name
    for table in tables:
        assert table["table_schema"] == "public"


def test_get_table_columns(postgres_conn, load_mock_schema):
    """Test getting columns for a specific table."""
    _ = load_mock_schema

    # Test users table
    columns = postgres_conn.get_table_columns("users")

    column_names = {col["name"] for col in columns}
    assert "id" in column_names
    assert "name" in column_names
    assert "email" in column_names
    assert "created_at" in column_names

    # Check column types
    id_col = next(col for col in columns if col["name"] == "id")
    assert "serial" in id_col["type"].lower() or "int4" in id_col["type"].lower()

    name_col = next(col for col in columns if col["name"] == "name")
    assert (
        "varchar" in name_col["type"].lower()
        or "character varying" in name_col["type"].lower()
    )

    # Check descriptions
    id_col = next(col for col in columns if col["name"] == "id")
    assert "identifier" in id_col["description"].lower() or id_col["description"] == ""

    # Test products table
    product_columns = postgres_conn.get_table_columns("products")
    product_column_names = {col["name"] for col in product_columns}
    assert "id" in product_column_names
    assert "name" in product_column_names
    assert "price" in product_column_names
    assert "description" in product_column_names


def test_get_primary_keys(postgres_conn, load_mock_schema):
    """Test getting primary key columns."""
    _ = load_mock_schema

    # Users table should have id as primary key
    pk_users = postgres_conn.get_primary_keys("users")
    assert len(pk_users) == 1
    assert "id" in pk_users

    # Products table should have id as primary key
    pk_products = postgres_conn.get_primary_keys("products")
    assert len(pk_products) == 1
    assert "id" in pk_products

    # Purchases table should have id as primary key
    pk_purchases = postgres_conn.get_primary_keys("purchases")
    assert len(pk_purchases) == 1
    assert "id" in pk_purchases

    # Follows table should have id as primary key
    pk_follows = postgres_conn.get_primary_keys("follows")
    assert len(pk_follows) == 1
    assert "id" in pk_follows


def test_get_foreign_keys(postgres_conn, load_mock_schema):
    """Test getting foreign key relationships."""
    _ = load_mock_schema

    # Users table should have no foreign keys
    fk_users = postgres_conn.get_foreign_keys("users")
    assert len(fk_users) == 0

    # Products table should have no foreign keys
    fk_products = postgres_conn.get_foreign_keys("products")
    assert len(fk_products) == 0

    # Purchases table should have 2 foreign keys
    fk_purchases = postgres_conn.get_foreign_keys("purchases")
    assert len(fk_purchases) == 2

    fk_columns = {fk["column"] for fk in fk_purchases}
    assert "user_id" in fk_columns
    assert "product_id" in fk_columns

    # Check references
    user_fk = next(fk for fk in fk_purchases if fk["column"] == "user_id")
    assert user_fk["references_table"] == "users"
    assert user_fk["references_column"] == "id"

    product_fk = next(fk for fk in fk_purchases if fk["column"] == "product_id")
    assert product_fk["references_table"] == "products"
    assert product_fk["references_column"] == "id"

    # Follows table should have 2 foreign keys (both to users)
    fk_follows = postgres_conn.get_foreign_keys("follows")
    assert len(fk_follows) == 2

    fk_columns = {fk["column"] for fk in fk_follows}
    assert "follower_id" in fk_columns
    assert "followed_id" in fk_columns

    # Both should reference users table
    for fk in fk_follows:
        assert fk["references_table"] == "users"
        assert fk["references_column"] == "id"


def test_detect_vertex_tables(postgres_conn, load_mock_schema):
    """Test detection of vertex-like tables."""
    _ = load_mock_schema

    vertex_tables = postgres_conn.detect_vertex_tables()

    # Should detect users and products as vertex tables
    vertex_table_names = {vt["name"] for vt in vertex_tables}
    assert "users" in vertex_table_names
    assert "products" in vertex_table_names

    # Purchases and follows should NOT be in vertex tables (they have exactly 2 FKs)
    assert "purchases" not in vertex_table_names
    assert "follows" not in vertex_table_names

    # Verify structure of vertex tables
    users_table = next(vt for vt in vertex_tables if vt["name"] == "users")
    assert "id" in users_table["primary_key"]
    assert len(users_table["foreign_keys"]) == 0
    assert len(users_table["columns"]) > 0

    # Check that columns have is_pk flag set
    id_col = next(col for col in users_table["columns"] if col["name"] == "id")
    assert id_col["is_pk"] is True

    name_col = next(col for col in users_table["columns"] if col["name"] == "name")
    assert name_col["is_pk"] is False


def test_detect_edge_tables(postgres_conn, load_mock_schema):
    """Test detection of edge-like tables."""
    _ = load_mock_schema

    edge_tables = postgres_conn.detect_edge_tables()

    # Should detect purchases and follows as edge tables
    edge_table_names = {et["name"] for et in edge_tables}
    assert "purchases" in edge_table_names
    assert "follows" in edge_table_names

    # Users and products should NOT be in edge tables
    assert "users" not in edge_table_names
    assert "products" not in edge_table_names

    # Verify structure of edge tables
    purchases_table = next(et for et in edge_tables if et["name"] == "purchases")
    assert purchases_table["source_table"] == "users"
    assert purchases_table["target_table"] == "products"
    assert purchases_table["source_column"] == "user_id"
    assert purchases_table["target_column"] == "product_id"
    assert len(purchases_table["foreign_keys"]) == 2

    follows_table = next(et for et in edge_tables if et["name"] == "follows")
    assert follows_table["source_table"] == "users"
    assert follows_table["target_table"] == "users"  # Self-referential
    assert follows_table["source_column"] in ["follower_id", "followed_id"]
    assert follows_table["target_column"] in ["follower_id", "followed_id"]
    assert len(follows_table["foreign_keys"]) == 2


def test_introspect_schema(postgres_conn, load_mock_schema):
    """Test the main introspection method."""
    _ = load_mock_schema

    schema_info = postgres_conn.introspect_schema()

    # Check structure
    assert "vertex_tables" in schema_info
    assert "edge_tables" in schema_info
    assert "schema_name" in schema_info
    assert schema_info["schema_name"] == "public"

    # Check vertex tables
    vertex_table_names = {vt["name"] for vt in schema_info["vertex_tables"]}
    assert "users" in vertex_table_names
    assert "products" in vertex_table_names
    assert len(schema_info["vertex_tables"]) == 2

    # Check edge tables
    edge_table_names = {et["name"] for et in schema_info["edge_tables"]}
    assert "purchases" in edge_table_names
    assert "follows" in edge_table_names
    assert len(schema_info["edge_tables"]) == 2

    # Verify that all tables have proper structure
    for vt in schema_info["vertex_tables"]:
        assert "name" in vt
        assert "schema" in vt
        assert "columns" in vt
        assert "primary_key" in vt
        assert "foreign_keys" in vt
        assert len(vt["primary_key"]) > 0

        # Check that columns have is_pk flag
        for col in vt["columns"]:
            assert "is_pk" in col

    for et in schema_info["edge_tables"]:
        assert "name" in et
        assert "schema" in et
        assert "columns" in et
        assert "primary_key" in et
        assert "foreign_keys" in et
        assert "source_table" in et
        assert "target_table" in et
        assert "source_column" in et
        assert "target_column" in et
        assert len(et["foreign_keys"]) == 2

        # Check that columns have is_pk flag
        for col in et["columns"]:
            assert "is_pk" in col


def test_introspect_schema_with_custom_schema(postgres_conn, load_mock_schema):
    """Test introspection with a custom schema name."""
    _ = load_mock_schema

    # Test with explicit schema name
    schema_info = postgres_conn.introspect_schema(schema_name="public")

    assert schema_info["schema_name"] == "public"
    assert len(schema_info["vertex_tables"]) == 2
    assert len(schema_info["edge_tables"]) == 2


def test_connection_close(postgres_conn):
    """Test that connection can be closed properly."""
    # Connection should be open
    assert postgres_conn.conn is not None

    # Close it
    postgres_conn.close()

    # Connection should be closed (checking by trying to use it would raise an error)
    # We can't easily check if it's closed without trying to use it, so we just verify
    # the method doesn't raise an exception
    assert True
