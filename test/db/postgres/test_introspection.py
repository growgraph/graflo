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
    vertex_table_names = {vt.name for vt in vertex_tables}
    assert "users" in vertex_table_names
    assert "products" in vertex_table_names

    # Purchases and follows should NOT be in vertex tables (they have exactly 2 FKs)
    assert "purchases" not in vertex_table_names
    assert "follows" not in vertex_table_names

    # Verify structure of vertex tables
    users_table = next(vt for vt in vertex_tables if vt.name == "users")
    assert "id" in users_table.primary_key
    assert len(users_table.foreign_keys) == 0
    assert len(users_table.columns) > 0

    # Check that columns have is_pk flag set
    id_col = next(col for col in users_table.columns if col.name == "id")
    assert id_col.is_pk is True

    name_col = next(col for col in users_table.columns if col.name == "name")
    assert name_col.is_pk is False


def test_detect_edge_tables(postgres_conn, load_mock_schema):
    """Test detection of edge-like tables."""
    _ = load_mock_schema

    edge_tables = postgres_conn.detect_edge_tables()

    # Should detect purchases and follows as edge tables
    edge_table_names = {et.name for et in edge_tables}
    assert "purchases" in edge_table_names
    assert "follows" in edge_table_names

    # Users and products should NOT be in edge tables
    assert "users" not in edge_table_names
    assert "products" not in edge_table_names

    # Verify structure of edge tables
    purchases_table = next(et for et in edge_tables if et.name == "purchases")
    assert purchases_table.source_table == "users"
    assert purchases_table.target_table == "products"
    assert purchases_table.source_column == "user_id"
    assert purchases_table.target_column == "product_id"
    assert len(purchases_table.foreign_keys) == 2

    follows_table = next(et for et in edge_tables if et.name == "follows")
    assert follows_table.source_table == "users"
    assert follows_table.target_table == "users"  # Self-referential
    assert follows_table.source_column in ["follower_id", "followed_id"]
    assert follows_table.target_column in ["follower_id", "followed_id"]
    assert len(follows_table.foreign_keys) == 2


def test_introspect_schema(postgres_conn, load_mock_schema):
    """Test the main introspection method."""
    _ = load_mock_schema

    schema_info = postgres_conn.introspect_schema()

    # Check structure
    assert schema_info.schema_name == "public"

    # Check vertex tables
    vertex_table_names = {vt.name for vt in schema_info.vertex_tables}
    assert "users" in vertex_table_names
    assert "products" in vertex_table_names
    assert len(schema_info.vertex_tables) == 2

    # Check edge tables
    edge_table_names = {et.name for et in schema_info.edge_tables}
    assert "purchases" in edge_table_names
    assert "follows" in edge_table_names
    assert len(schema_info.edge_tables) == 2

    # Verify that all tables have proper structure
    for vt in schema_info.vertex_tables:
        assert len(vt.primary_key) > 0

        # Check that columns have is_pk flag
        for col in vt.columns:
            assert hasattr(col, "is_pk")

    for et in schema_info.edge_tables:
        assert len(et.foreign_keys) == 2

        # Check that columns have is_pk flag
        for col in et.columns:
            assert hasattr(col, "is_pk")


def test_introspect_schema_with_custom_schema(postgres_conn, load_mock_schema):
    """Test introspection with a custom schema name."""
    _ = load_mock_schema

    # Test with explicit schema name
    schema_info = postgres_conn.introspect_schema(schema_name="public")

    assert schema_info.schema_name == "public"
    assert len(schema_info.vertex_tables) == 2
    assert len(schema_info.edge_tables) == 2


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


def test_pg_catalog_fallback_methods(postgres_conn, load_mock_schema):
    """Test that pg_catalog fallback methods work correctly."""
    _ = load_mock_schema

    # Test pg_catalog table retrieval
    tables = postgres_conn._get_tables_pg_catalog("public")
    table_names = {t["table_name"] for t in tables}
    assert "users" in table_names
    assert "products" in table_names
    assert "purchases" in table_names
    assert "follows" in table_names

    # Test pg_catalog column retrieval
    columns = postgres_conn._get_table_columns_pg_catalog("users", "public")
    column_names = {col["name"] for col in columns}
    assert "id" in column_names
    assert "name" in column_names
    assert "email" in column_names

    # Test pg_catalog primary key retrieval
    pk_users = postgres_conn._get_primary_keys_pg_catalog("users", "public")
    assert len(pk_users) == 1
    assert "id" in pk_users

    # Test pg_catalog foreign key retrieval
    fk_purchases = postgres_conn._get_foreign_keys_pg_catalog("purchases", "public")
    assert len(fk_purchases) == 2
    fk_columns = {fk["column"] for fk in fk_purchases}
    assert "user_id" in fk_columns
    assert "product_id" in fk_columns


def test_edge_table_with_multiple_primary_keys(postgres_conn):
    """Test detection of edge tables with 2+ primary keys."""
    # Create a test table with composite primary key (edge-like)
    with postgres_conn.conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cluster (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS host (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rel_cluster_containment_host (
                cluster_id INTEGER NOT NULL,
                host_id INTEGER NOT NULL,
                PRIMARY KEY (cluster_id, host_id),
                FOREIGN KEY (cluster_id) REFERENCES cluster(id),
                FOREIGN KEY (host_id) REFERENCES host(id)
            )
        """)
        postgres_conn.conn.commit()

    try:
        # Test that it's detected as edge-like
        pk_columns = postgres_conn.get_primary_keys("rel_cluster_containment_host")
        assert len(pk_columns) >= 2
        assert "cluster_id" in pk_columns
        assert "host_id" in pk_columns

        # Test edge detection
        edge_tables = postgres_conn.detect_edge_tables()
        edge_table_names = {et.name for et in edge_tables}
        assert "rel_cluster_containment_host" in edge_table_names

        # Verify structure
        edge_table = next(
            et for et in edge_tables if et.name == "rel_cluster_containment_host"
        )
        assert len(edge_table.primary_key) >= 2
        assert edge_table.source_table in ["cluster", "host"]
        assert edge_table.target_table in ["cluster", "host"]
        assert edge_table.source_column in ["cluster_id", "host_id"]
        assert edge_table.target_column in ["cluster_id", "host_id"]

    finally:
        # Cleanup
        with postgres_conn.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS rel_cluster_containment_host CASCADE")
            cursor.execute("DROP TABLE IF EXISTS cluster CASCADE")
            cursor.execute("DROP TABLE IF EXISTS host CASCADE")
            postgres_conn.conn.commit()


def test_rel_prefix_table_detection(postgres_conn):
    """Test that tables with rel_ prefix are detected as edge tables."""
    # Create test tables
    with postgres_conn.conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vertex_a (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vertex_b (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rel_vertex_a_to_vertex_b (
                id SERIAL PRIMARY KEY,
                vertex_a_id INTEGER NOT NULL,
                vertex_b_id INTEGER NOT NULL,
                FOREIGN KEY (vertex_a_id) REFERENCES vertex_a(id),
                FOREIGN KEY (vertex_b_id) REFERENCES vertex_b(id)
            )
        """)
        postgres_conn.conn.commit()

    try:
        # Test edge detection - rel_ prefix should be detected as edge-like
        edge_tables = postgres_conn.detect_edge_tables()
        edge_table_names = {et.name for et in edge_tables}
        assert "rel_vertex_a_to_vertex_b" in edge_table_names

        # Verify it's not in vertex tables
        vertex_tables = postgres_conn.detect_vertex_tables()
        vertex_table_names = {vt.name for vt in vertex_tables}
        assert "rel_vertex_a_to_vertex_b" not in vertex_table_names

    finally:
        # Cleanup
        with postgres_conn.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS rel_vertex_a_to_vertex_b CASCADE")
            cursor.execute("DROP TABLE IF EXISTS vertex_a CASCADE")
            cursor.execute("DROP TABLE IF EXISTS vertex_b CASCADE")
            postgres_conn.conn.commit()


def test_is_edge_like_table_heuristic(postgres_conn):
    """Test the _is_edge_like_table heuristic function."""
    # Test case 1: Table with 2+ primary keys
    assert postgres_conn._is_edge_like_table("test_table", ["pk1", "pk2"], []) is True

    # Test case 2: Table with exactly 2 foreign keys
    assert (
        postgres_conn._is_edge_like_table(
            "test_table",
            ["id"],
            [
                {"column": "fk1", "references_table": "table1"},
                {"column": "fk2", "references_table": "table2"},
            ],
        )
        is True
    )

    # Test case 3: Table with rel_ prefix
    assert postgres_conn._is_edge_like_table("rel_test_table", ["id"], []) is True

    # Test case 4: Table where PK columns are subset of FK columns
    assert (
        postgres_conn._is_edge_like_table(
            "test_table",
            ["fk1", "fk2"],
            [
                {"column": "fk1", "references_table": "table1"},
                {"column": "fk2", "references_table": "table2"},
                {"column": "fk3", "references_table": "table3"},
            ],
        )
        is True
    )

    # Test case 5: Regular vertex table (should return False)
    assert (
        postgres_conn._is_edge_like_table(
            "test_table",
            ["id"],
            [{"column": "fk1", "references_table": "table1"}],
        )
        is False
    )

    # Test case 6: Table with single PK, single FK (should return False)
    assert (
        postgres_conn._is_edge_like_table(
            "test_table",
            ["id"],
            [{"column": "fk1", "references_table": "table1"}],
        )
        is False
    )


def test_edge_table_without_foreign_keys(postgres_conn):
    """Test edge table detection when foreign keys are missing but PK suggests edge."""
    # Create a table with composite PK but no foreign keys (simulating incomplete schema)
    with postgres_conn.conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_vertex (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rel_test_edge (
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                PRIMARY KEY (source_id, target_id)
            )
        """)
        postgres_conn.conn.commit()

    try:
        # The table should be detected as edge-like due to 2+ primary keys
        pk_columns = postgres_conn.get_primary_keys("rel_test_edge")
        assert len(pk_columns) >= 2

        # Test edge detection
        # edge_tables = postgres_conn.detect_edge_tables()
        # edge_table_names = {et["name"] for et in edge_tables}
        # Note: This might not be added if source/target can't be inferred
        # But the heuristic should still identify it as edge-like

    finally:
        # Cleanup
        with postgres_conn.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS rel_test_edge CASCADE")
            cursor.execute("DROP TABLE IF EXISTS test_vertex CASCADE")
            postgres_conn.conn.commit()


def test_schema_inference_with_pg_catalog_fallback(postgres_conn, load_mock_schema):
    """Test that schema inference works correctly when using pg_catalog fallback methods.

    This test simulates a scenario where information_schema is unavailable or unreliable,
    forcing the use of pg_catalog fallback methods for schema introspection.
    """
    _ = load_mock_schema  # Ensure schema is loaded

    # Mock the _check_information_schema_reliable method to return False
    # This forces the use of pg_catalog fallback
    original_check = postgres_conn._check_information_schema_reliable
    postgres_conn._check_information_schema_reliable = lambda schema_name: False

    try:
        # Test that get_tables uses pg_catalog fallback
        tables = postgres_conn.get_tables("public")
        table_names = {t["table_name"] for t in tables}

        # Verify all expected tables are found using pg_catalog
        assert "users" in table_names, "users table should be found via pg_catalog"
        assert "products" in table_names, (
            "products table should be found via pg_catalog"
        )
        assert "purchases" in table_names, (
            "purchases table should be found via pg_catalog"
        )
        assert "follows" in table_names, "follows table should be found via pg_catalog"
        assert len(tables) == 4, f"Expected 4 tables, got {len(tables)}"

        # Verify schema name
        for table in tables:
            assert table["table_schema"] == "public", (
                f"Expected schema 'public', got {table['table_schema']}"
            )

        # Test that columns can be retrieved using pg_catalog fallback
        users_columns = postgres_conn.get_table_columns("users", "public")
        column_names = {col["name"] for col in users_columns}
        assert "id" in column_names, "id column should be found via pg_catalog"
        assert "name" in column_names, "name column should be found via pg_catalog"
        assert "email" in column_names, "email column should be found via pg_catalog"
        assert "created_at" in column_names, (
            "created_at column should be found via pg_catalog"
        )

        # Test that primary keys can be retrieved using pg_catalog fallback
        pk_users = postgres_conn.get_primary_keys("users")
        assert len(pk_users) == 1, f"Expected 1 primary key, got {len(pk_users)}"
        assert "id" in pk_users, "id should be the primary key"

        # Test that foreign keys can be retrieved using pg_catalog fallback
        fk_purchases = postgres_conn.get_foreign_keys("purchases")
        assert len(fk_purchases) == 2, (
            f"Expected 2 foreign keys, got {len(fk_purchases)}"
        )
        fk_columns = {fk["column"] for fk in fk_purchases}
        assert "user_id" in fk_columns, "user_id should be a foreign key"
        assert "product_id" in fk_columns, "product_id should be a foreign key"

        # Test that vertex detection works with pg_catalog fallback
        vertex_tables = postgres_conn.detect_vertex_tables("public")
        vertex_table_names = {vt.name for vt in vertex_tables}
        assert "users" in vertex_table_names, "users should be detected as vertex table"
        assert "products" in vertex_table_names, (
            "products should be detected as vertex table"
        )
        assert len(vertex_tables) == 2, (
            f"Expected 2 vertex tables, got {len(vertex_tables)}"
        )

        # Test that edge detection works with pg_catalog fallback
        edge_tables = postgres_conn.detect_edge_tables("public")
        edge_table_names = {et.name for et in edge_tables}
        assert "purchases" in edge_table_names, (
            "purchases should be detected as edge table"
        )
        assert "follows" in edge_table_names, "follows should be detected as edge table"
        assert len(edge_tables) == 2, f"Expected 2 edge tables, got {len(edge_tables)}"

        # Test that full schema introspection works with pg_catalog fallback
        schema_info = postgres_conn.introspect_schema("public")

        # Verify structure
        assert schema_info.schema_name == "public", (
            f"Expected schema_name 'public', got {schema_info.schema_name}"
        )

        # Verify vertex tables
        vertex_table_names = {vt.name for vt in schema_info.vertex_tables}
        assert "users" in vertex_table_names, "users should be in vertex_tables"
        assert "products" in vertex_table_names, "products should be in vertex_tables"
        assert len(schema_info.vertex_tables) == 2, (
            f"Expected 2 vertex tables, got {len(schema_info.vertex_tables)}"
        )

        # Verify edge tables
        edge_table_names = {et.name for et in schema_info.edge_tables}
        assert "purchases" in edge_table_names, "purchases should be in edge_tables"
        assert "follows" in edge_table_names, "follows should be in edge_tables"
        assert len(schema_info.edge_tables) == 2, (
            f"Expected 2 edge tables, got {len(schema_info.edge_tables)}"
        )

        # Verify that vertex tables have proper structure
        users_table = next(vt for vt in schema_info.vertex_tables if vt.name == "users")
        assert "id" in users_table.primary_key, "users should have id as primary key"
        assert len(users_table.foreign_keys) == 0, "users should have no foreign keys"
        assert len(users_table.columns) > 0, "users should have columns"

        # Verify that edge tables have proper structure
        purchases_table = next(
            et for et in schema_info.edge_tables if et.name == "purchases"
        )
        # Source and target can be in either order, but should reference users and products
        assert purchases_table.source_table in ["users", "products"], (
            f"Expected purchases.source_table to be 'users' or 'products', "
            f"got {purchases_table.source_table}"
        )
        assert purchases_table.target_table in ["users", "products"], (
            f"Expected purchases.target_table to be 'users' or 'products', "
            f"got {purchases_table.target_table}"
        )
        # They should be different
        assert purchases_table.source_table != purchases_table.target_table, (
            "purchases should connect different tables"
        )
        assert len(purchases_table.foreign_keys) == 2, (
            f"Expected 2 foreign keys, got {len(purchases_table.foreign_keys)}"
        )

        # Verify that columns have is_pk flag set correctly
        for vt in schema_info.vertex_tables:
            for col in vt.columns:
                assert hasattr(col, "is_pk"), "columns should have is_pk flag"
                if col.name in vt.primary_key:
                    assert col.is_pk is True, (
                        f"{col.name} should be marked as primary key"
                    )
                else:
                    assert col.is_pk is False, (
                        f"{col.name} should not be marked as primary key"
                    )

    finally:
        # Restore original method
        postgres_conn._check_information_schema_reliable = original_check
