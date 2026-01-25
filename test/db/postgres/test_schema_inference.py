"""Tests for PostgreSQL schema inference functionality.

This module tests the schema inference capabilities, including:
- GraphEngine.infer_schema_from_postgres() method
- Vertex and edge detection
- Field type mapping
- Resource creation
"""

from unittest.mock import patch

from graflo.hq import GraphEngine
from graflo.onto import DBFlavor


def test_infer_schema_from_postgres(conn_conf, load_mock_schema):
    """Test that infer_schema_from_postgres correctly infers schema from PostgreSQL."""
    _ = load_mock_schema  # Ensure schema is loaded

    engine = GraphEngine(target_db_flavor=DBFlavor.ARANGO)
    schema = engine.infer_schema(conn_conf, schema_name="public")

    # Verify schema structure
    assert schema is not None
    assert schema.vertex_config is not None
    assert schema.edge_config is not None
    assert schema.general is not None

    # Check vertex tables (users, products)
    vertex_names = [v.name for v in schema.vertex_config.vertices]
    assert "users" in vertex_names, f"Expected 'users' in vertices, got {vertex_names}"
    assert "products" in vertex_names, (
        f"Expected 'products' in vertices, got {vertex_names}"
    )

    # Check edge tables (purchases, follows)
    # Edge objects are accessed via _edges_map, which uses edge_id (source, target, relation) as key
    edge_ids = list(schema.edge_config._edges_map.keys())
    # Find edges by checking source/target combinations
    purchases_found = any(
        e.source == "users" and e.target == "products"
        for e in schema.edge_config._edges_map.values()
    )
    follows_found = any(
        e.source == "users" and e.target == "users"
        for e in schema.edge_config._edges_map.values()
    )
    assert purchases_found, (
        f"Expected purchases edge (users -> products), got edges: {edge_ids}"
    )
    assert follows_found, (
        f"Expected follows edge (users -> users), got edges: {edge_ids}"
    )

    # Verify users vertex fields
    users_vertex = next(v for v in schema.vertex_config.vertices if v.name == "users")
    field_names = [f.name for f in users_vertex.fields]
    assert "id" in field_names, f"Expected 'id' in users fields, got {field_names}"
    assert "name" in field_names, f"Expected 'name' in users fields, got {field_names}"
    assert "email" in field_names, (
        f"Expected 'email' in users fields, got {field_names}"
    )

    # Verify field types (id should be INT, name/email should be STRING)
    id_field = next(f for f in users_vertex.fields if f.name == "id")
    assert id_field.type is not None, "id field should have a type"
    assert id_field.type.value == "INT", (
        f"Expected id type to be INT, got {id_field.type.value}"
    )

    name_field = next(f for f in users_vertex.fields if f.name == "name")
    assert name_field.type is not None, "name field should have a type"
    assert name_field.type.value == "STRING", (
        f"Expected name type to be STRING, got {name_field.type.value}"
    )

    # Verify datetime field type (created_at should be DATETIME)
    created_at_field = next(
        (f for f in users_vertex.fields if f.name == "created_at"), None
    )
    if created_at_field:
        assert created_at_field.type is not None, "created_at field should have a type"
        assert created_at_field.type.value == "DATETIME", (
            f"Expected created_at type to be DATETIME, got {created_at_field.type.value}"
        )

    # Verify purchases edge structure
    purchases_edge = next(
        (
            e
            for e in schema.edge_config._edges_map.values()
            if e.source == "users" and e.target == "products"
        ),
        None,
    )
    assert purchases_edge is not None, "purchases edge should exist"
    assert purchases_edge.source == "users", (
        f"Expected purchases.source to be 'users', got {purchases_edge.source}"
    )
    assert purchases_edge.target == "products", (
        f"Expected purchases.target to be 'products', got {purchases_edge.target}"
    )

    # Verify edge has weight configuration if applicable
    # (purchases might have quantity or price as weight)
    if purchases_edge.weights:
        # WeightConfig has 'direct' list for direct weights
        assert (
            len(purchases_edge.weights.direct) > 0
            or len(purchases_edge.weights.vertices) > 0
        ), "purchases edge should have weights"

    # Verify resources were created
    assert len(schema.resources) > 0, "Schema should have resources"
    resource_names = [r.name for r in schema.resources]
    assert "users" in resource_names, f"Expected 'users' resource, got {resource_names}"
    assert "products" in resource_names, (
        f"Expected 'products' resource, got {resource_names}"
    )
    assert "purchases" in resource_names, (
        f"Expected 'purchases' resource, got {resource_names}"
    )
    assert "follows" in resource_names, (
        f"Expected 'follows' resource, got {resource_names}"
    )

    # Verify resource actors
    users_resource = next(r for r in schema.resources if r.name == "users")
    assert users_resource.apply is not None, "users resource should have apply list"
    assert len(users_resource.apply) > 0, (
        "users resource should have at least one actor"
    )

    purchases_resource = next(r for r in schema.resources if r.name == "purchases")
    assert purchases_resource.apply is not None, (
        "purchases resource should have apply list"
    )
    assert len(purchases_resource.apply) > 0, (
        "purchases resource should have at least one actor"
    )

    print("\n" + "=" * 80)
    print("Schema Inference Results:")
    print("=" * 80)
    print(f"\nVertices ({len(schema.vertex_config.vertices)}):")
    for v in schema.vertex_config.vertices:
        field_types = ", ".join(
            [f"{f.name}:{f.type.value if f.type else 'None'}" for f in v.fields[:5]]
        )
        print(f"  - {v.name}: {field_types}...")

    print(f"\nEdges ({len(schema.edge_config._edges_map)}):")
    for edge_id, e in schema.edge_config._edges_map.items():
        weights_info = ""
        if e.weights:
            weight_count = len(e.weights.direct) + len(e.weights.vertices)
            weights_info = f" (weights: {weight_count})"
        relation_info = f" [{e.relation}]" if e.relation else ""
        print(f"  - {edge_id}: {e.source} -> {e.target}{relation_info}{weights_info}")

    print(f"\nResources ({len(schema.resources)}):")
    for r in schema.resources:
        actor_types = [type(a).__name__ for a in r.apply]
        print(f"  - {r.name} (actors: {', '.join(actor_types)})")

    print("=" * 80)


def test_infer_schema_with_pg_catalog_fallback(conn_conf, load_mock_schema):
    """Test that schema inference works correctly when using pg_catalog fallback methods.

    This test simulates a scenario where information_schema is unavailable or unreliable,
    forcing the use of pg_catalog fallback methods. It verifies that the complete schema
    inference pipeline (from PostgreSQL schema to graflo Schema) works correctly
    using only pg_catalog methods.
    """
    _ = load_mock_schema  # Ensure schema is loaded

    # Mock the _check_information_schema_reliable method to return False
    # This forces the use of pg_catalog fallback throughout the introspection process
    # Since infer_schema() creates its own connection, we need to patch the class method
    from graflo.db.postgres import PostgresConnection

    with patch.object(
        PostgresConnection, "_check_information_schema_reliable", return_value=False
    ):
        # Test that infer_schema_from_postgres works with pg_catalog fallback
        engine = GraphEngine(target_db_flavor=DBFlavor.ARANGO)
        schema = engine.infer_schema(conn_conf, schema_name="public")

        # Verify schema structure
        assert schema is not None, "Schema should be inferred"
        assert schema.vertex_config is not None, "Schema should have vertex_config"
        assert schema.edge_config is not None, "Schema should have edge_config"
        assert schema.general is not None, "Schema should have general metadata"
        assert schema.general.name == "public", (
            f"Expected schema name 'public', got {schema.general.name}"
        )

        # Check vertex tables (users, products) - should be detected correctly via pg_catalog
        vertex_names = [v.name for v in schema.vertex_config.vertices]
        assert "users" in vertex_names, (
            f"Expected 'users' in vertices when using pg_catalog, got {vertex_names}"
        )
        assert "products" in vertex_names, (
            f"Expected 'products' in vertices when using pg_catalog, got {vertex_names}"
        )
        assert len(vertex_names) == 2, (
            f"Expected 2 vertices when using pg_catalog, got {len(vertex_names)}"
        )

        # Check edge tables (purchases, follows) - should be detected correctly via pg_catalog
        edge_ids = list(schema.edge_config._edges_map.keys())
        # Purchases edge can be in either direction (users -> products or products -> users)
        purchases_found = any(
            (e.source == "users" and e.target == "products")
            or (e.source == "products" and e.target == "users")
            for e in schema.edge_config._edges_map.values()
        )
        follows_found = any(
            e.source == "users" and e.target == "users"
            for e in schema.edge_config._edges_map.values()
        )
        assert purchases_found, (
            f"Expected purchases edge (users <-> products) when using pg_catalog, "
            f"got edges: {edge_ids}"
        )
        assert follows_found, (
            f"Expected follows edge (users -> users) when using pg_catalog, "
            f"got edges: {edge_ids}"
        )

        # Verify users vertex fields - should be correctly inferred via pg_catalog
        users_vertex = next(
            v for v in schema.vertex_config.vertices if v.name == "users"
        )
        field_names = [f.name for f in users_vertex.fields]
        assert "id" in field_names, (
            f"Expected 'id' in users fields when using pg_catalog, got {field_names}"
        )
        assert "name" in field_names, (
            f"Expected 'name' in users fields when using pg_catalog, got {field_names}"
        )
        assert "email" in field_names, (
            f"Expected 'email' in users fields when using pg_catalog, got {field_names}"
        )

        # Verify field types - should be correctly mapped via pg_catalog
        id_field = next(f for f in users_vertex.fields if f.name == "id")
        assert id_field.type is not None, (
            "id field should have a type when using pg_catalog"
        )
        assert id_field.type.value == "INT", (
            f"Expected id type to be INT when using pg_catalog, got {id_field.type.value}"
        )

        name_field = next(f for f in users_vertex.fields if f.name == "name")
        assert name_field.type is not None, (
            "name field should have a type when using pg_catalog"
        )
        assert name_field.type.value == "STRING", (
            f"Expected name type to be STRING when using pg_catalog, got {name_field.type.value}"
        )

        # Verify purchases edge structure - should be correctly inferred via pg_catalog
        # Edge direction can vary, but should connect users and products
        purchases_edge = next(
            (
                e
                for e in schema.edge_config._edges_map.values()
                if (e.source == "users" and e.target == "products")
                or (e.source == "products" and e.target == "users")
            ),
            None,
        )
        assert purchases_edge is not None, (
            "purchases edge should exist when using pg_catalog"
        )
        assert purchases_edge.source in ["users", "products"], (
            f"Expected purchases.source to be 'users' or 'products' when using pg_catalog, "
            f"got {purchases_edge.source}"
        )
        assert purchases_edge.target in ["users", "products"], (
            f"Expected purchases.target to be 'users' or 'products' when using pg_catalog, "
            f"got {purchases_edge.target}"
        )
        assert purchases_edge.source != purchases_edge.target, (
            "purchases edge should connect different tables"
        )

        # Verify resources were created - should work correctly via pg_catalog
        assert len(schema.resources) > 0, (
            "Schema should have resources when using pg_catalog"
        )
        resource_names = [r.name for r in schema.resources]
        assert "users" in resource_names, (
            f"Expected 'users' resource when using pg_catalog, got {resource_names}"
        )
        assert "products" in resource_names, (
            f"Expected 'products' resource when using pg_catalog, got {resource_names}"
        )
        assert "purchases" in resource_names, (
            f"Expected 'purchases' resource when using pg_catalog, got {resource_names}"
        )
        assert "follows" in resource_names, (
            f"Expected 'follows' resource when using pg_catalog, got {resource_names}"
        )

        # Verify resource actors - should be correctly created via pg_catalog
        users_resource = next(r for r in schema.resources if r.name == "users")
        assert users_resource.apply is not None, (
            "users resource should have apply list when using pg_catalog"
        )
        assert len(users_resource.apply) > 0, (
            "users resource should have at least one actor when using pg_catalog"
        )

        purchases_resource = next(r for r in schema.resources if r.name == "purchases")
        assert purchases_resource.apply is not None, (
            "purchases resource should have apply list when using pg_catalog"
        )
        assert len(purchases_resource.apply) > 0, (
            "purchases resource should have at least one actor when using pg_catalog"
        )

        print("\n" + "=" * 80)
        print("Schema Inference Results (using pg_catalog fallback):")
        print("=" * 80)
        print(f"\nVertices ({len(schema.vertex_config.vertices)}):")
        for v in schema.vertex_config.vertices:
            field_types = ", ".join(
                [f"{f.name}:{f.type.value if f.type else 'None'}" for f in v.fields[:5]]
            )
            print(f"  - {v.name}: {field_types}...")

        print(f"\nEdges ({len(schema.edge_config._edges_map)}):")
        for edge_id, e in schema.edge_config._edges_map.items():
            weights_info = ""
            if e.weights:
                weight_count = len(e.weights.direct) + len(e.weights.vertices)
                weights_info = f" (weights: {weight_count})"
            relation_info = f" [{e.relation}]" if e.relation else ""
            print(
                f"  - {edge_id}: {e.source} -> {e.target}{relation_info}{weights_info}"
            )

        print(f"\nResources ({len(schema.resources)}):")
        for r in schema.resources:
            actor_types = [type(a).__name__ for a in r.apply]
            print(f"  - {r.name} (actors: {', '.join(actor_types)})")

        print("=" * 80)
