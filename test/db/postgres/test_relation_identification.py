"""Tests for relation table identification logic in PostgreSQL connection."""

from graflo.db.sql.inference_utils import (
    _determine_source_target_vertices,
    _extract_fk_vertex_names,
    _extract_key_fragments,
    _identify_relation_name,
    _match_vertices_from_key_fragments,
    _match_vertices_from_table_fragments,
    detect_separator,
    fuzzy_match_fragment,
    infer_edge_vertices_from_table_name,
    split_by_separator,
)
from graflo.util.fuzzy_matcher import FuzzyMatcher


class TestRelationIdentification:
    """Test suite for relation table identification logic."""

    def test_detect_separator(self):
        """Test separator detection."""
        # Test underscore separator
        assert detect_separator("rel_plant_containment_machine") == "_"

        # Test hyphen separator
        assert detect_separator("rel-plant-containment-machine") == "-"

        # Test dot separator
        assert detect_separator("rel.plant.containment.machine") == "."

        # Test default when no separator
        assert detect_separator("relplantcontainmentmachine") == "_"

        # Test mixed separators (should pick most common)
        assert detect_separator("rel_plant-containment_machine") == "_"

    def test_split_by_separator(self):
        """Test splitting by separator."""
        # Test underscore separator
        assert split_by_separator("rel_plant_containment_machine", "_") == [
            "rel",
            "plant",
            "containment",
            "machine",
        ]

        # Test hyphen separator
        assert split_by_separator("rel-plant-containment-machine", "-") == [
            "rel",
            "plant",
            "containment",
            "machine",
        ]

        # Test multiple consecutive separators
        assert split_by_separator("rel__plant___containment", "_") == [
            "rel",
            "plant",
            "containment",
        ]

        # Test empty string
        assert split_by_separator("", "_") == []

    def test_fuzzy_match_fragment(self):
        """Test fuzzy matching of fragments to vertex names."""
        vertex_names = ["plant", "machine", "user", "product", "category"]

        # Test exact match (case-insensitive)
        assert fuzzy_match_fragment("plant", vertex_names) == "plant"
        assert fuzzy_match_fragment("PLANT", vertex_names) == "plant"

        # Test substring match
        assert fuzzy_match_fragment("plan", vertex_names) == "plant"
        assert fuzzy_match_fragment("machines", vertex_names) == "machine"

        # Test fuzzy match
        assert fuzzy_match_fragment("usr", vertex_names) == "user"
        assert fuzzy_match_fragment("prod", vertex_names) == "product"

        # Test no match (below threshold)
        assert fuzzy_match_fragment("xyz", vertex_names) is None

        # Test empty vertex names
        assert fuzzy_match_fragment("plant", []) is None

    def test_infer_edge_vertices_from_table_name_with_fks(self):
        """Test inference when foreign keys are available."""
        pk_columns = ["plant_id", "machine_id"]
        fk_columns = [
            {"column": "plant_id", "references_table": "plant"},
            {"column": "machine_id", "references_table": "machine"},
        ]
        vertex_names = ["plant", "machine", "user"]

        # Test with FK references (most reliable)
        source, target, relation = infer_edge_vertices_from_table_name(
            "prop_plant_containment_machine", pk_columns, fk_columns, vertex_names
        )
        assert source == "plant"
        assert target == "machine"
        assert relation == "containment"

        # Test self-reference
        fk_columns_self = [{"column": "user_id", "references_table": "user"}]
        source, target, relation = infer_edge_vertices_from_table_name(
            "prop_user_follows_user",
            ["user_id", "follows_user_id"],
            fk_columns_self,
            vertex_names,
        )
        assert source == "user"
        assert target == "user"
        assert relation == "follows"

    def test_infer_edge_vertices_from_table_name_fuzzy_matching(self):
        """Test inference using fuzzy matching when FKs are not available."""
        pk_columns = ["plant_id", "machine_id"]
        fk_columns = []
        vertex_names = ["plant", "machine", "user", "product", "category"]

        # Test fuzzy matching from table name
        source, target, relation = infer_edge_vertices_from_table_name(
            "prop_plant_containment_machine", pk_columns, fk_columns, vertex_names
        )
        assert source == "plant"
        assert target == "machine"
        assert relation == "containment"

        # Test with different separator
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel-plant-containment-machine", pk_columns, fk_columns, vertex_names
        )
        assert source == "plant"
        assert target == "machine"
        assert relation == "containment"

        # Test product_category_mapping connector
        source, target, relation = infer_edge_vertices_from_table_name(
            "product_category_mapping",
            ["product_id", "category_id"],
            fk_columns,
            vertex_names,
        )
        assert source == "product"
        assert target == "category"
        assert relation == "mapping"

    def test_infer_edge_vertices_from_key_fragments(self):
        """Test inference from key column fragments."""
        pk_columns = ["plant_id", "machine_id"]
        fk_columns = []
        vertex_names = ["plant", "machine"]

        # Test matching from PK column names
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_containment", pk_columns, fk_columns, vertex_names
        )
        assert source == "plant"
        assert target == "machine"
        assert relation == "containment"

        # Test with FK column fragments
        fk_columns_with_fragments = [
            {"column": "source_plant_id", "references_table": None},
            {"column": "target_machine_id", "references_table": None},
        ]
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_containment", pk_columns, fk_columns_with_fragments, vertex_names
        )
        assert source == "plant"
        assert target == "machine"
        assert relation == "containment"

    def test_infer_edge_vertices_priority(self):
        """Test that FK references take priority over fuzzy matching."""
        pk_columns = ["wrong_id", "also_wrong_id"]
        fk_columns = [
            {"column": "plant_id", "references_table": "plant"},
            {"column": "machine_id", "references_table": "machine"},
        ]
        vertex_names = ["plant", "machine", "wrong", "also_wrong"]

        # FK references should override fuzzy matches from table/column names
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_wrong_also_wrong", pk_columns, fk_columns, vertex_names
        )
        assert source == "plant"
        assert target == "machine"
        # Relation should still be inferred from table name
        assert relation is not None

    def test_infer_edge_vertices_no_match(self):
        """Test inference when no matches are found."""
        pk_columns = ["xyz_id", "abc_id"]
        fk_columns = []
        vertex_names = ["plant", "machine"]

        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_xyz_abc", pk_columns, fk_columns, vertex_names
        )
        assert source is None
        assert target is None
        assert relation is None

        # Test with empty vertex names
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_plant_machine", pk_columns, fk_columns, []
        )
        assert source is None
        assert target is None
        assert relation is None

    def test_infer_edge_vertices_complex_connectors(self):
        """Test inference with complex naming patterns."""
        vertex_names = ["plant", "machine", "user", "product", "category"]

        # Test pattern: rel_<source>_<relation>_<target>_<number>
        pk_columns = ["plant_id", "plant_id_2"]
        fk_columns = [
            {"column": "plant_id", "references_table": "plant"},
            {"column": "plant_id_2", "references_table": "plant"},
        ]
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_plant_containment_plant_2", pk_columns, fk_columns, vertex_names
        )
        assert source == "plant"
        assert target == "plant"
        assert relation == "containment"

        # Test pattern without rel_ prefix
        pk_columns = ["user_id", "follows_user_id"]
        fk_columns = []
        source, target, relation = infer_edge_vertices_from_table_name(
            "user_follows_user", pk_columns, fk_columns, vertex_names
        )
        assert source == "user"
        assert target == "user"
        assert relation == "follows"

    def test_robust_source_target_matching_patterns(self):
        """Test robust matching patterns: SOURCE_<relation>_TARGET and SOURCE_TARGET_<relation>.

        Tests the new matching logic:
        - Match source starting from the left
        - Match target starting from the right
        - Stop when we have 2 matches OR target_index > source_index + 1
        - Relation is derived from fragments that are neither source nor target
        """
        vertex_names = ["user", "product", "order", "customer", "item"]
        pk_columns = ["user_id", "product_id"]
        fk_columns = []

        # Pattern 1: somepattern_SOURCE_<relation>_TARGET
        # Example: "rel_user_purchases_product" -> user, product, purchases
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_user_purchases_product",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user", f"Expected source='user', got {source}"
        assert target == "product", f"Expected target='product', got {target}"
        assert relation == "purchases", f"Expected relation='purchases', got {relation}"

        # Pattern 2: somepattern_SOURCE_TARGET_<relation>
        # Example: "rel_user_product_purchase" -> user, product, purchase
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_user_product_purchase",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user", f"Expected source='user', got {source}"
        assert target == "product", f"Expected target='product', got {target}"
        assert relation == "purchase", f"Expected relation='purchase', got {relation}"

        # Pattern 3: With prefix before source
        # Example: "someprefix_user_orders_product" -> user, product, orders
        source, target, relation = infer_edge_vertices_from_table_name(
            "someprefix_user_orders_product",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user", f"Expected source='user', got {source}"
        assert target == "product", f"Expected target='product', got {target}"
        assert relation == "orders", f"Expected relation='orders', got {relation}"

        # Pattern 4: SOURCE_TARGET_<relation> with prefix
        # Example: "prefix_user_buys_order" -> user, buys, order
        source, target, relation = infer_edge_vertices_from_table_name(
            "prefix_user_buys_order",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user", f"Expected source='user', got {source}"
        assert target == "order", f"Expected target='product', got {target}"
        assert relation == "buys", f"Expected relation='order', got {relation}"

        # Pattern 5: Multiple fragments between source and target
        # Example: "user_has_many_products" -> user, products, has_many (or just "has" or "many")
        # This tests that we correctly identify relation fragments between source and target
        pk_columns = ["user_id", "product_id"]
        source, target, relation = infer_edge_vertices_from_table_name(
            "user_has_many_products",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user", f"Expected source='user', got {source}"
        assert target == "product", f"Expected target='product', got {target}"
        # Relation should be one of the fragments between source and target
        assert relation in ["has", "many"], (
            f"Expected relation in ['has', 'many'], got {relation}"
        )

        # Pattern 6: Self-reference with relation between
        # Example: "user_follows_user" -> user, user, follows
        pk_columns = ["user_id", "follows_user_id"]
        source, target, relation = infer_edge_vertices_from_table_name(
            "user_follows_user",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user", f"Expected source='user', got {source}"
        assert target == "user", f"Expected target='user', got {target}"
        assert relation == "follows", f"Expected relation='follows', got {relation}"

        # Pattern 7: Complex pattern with FK override (FKs are primary source of truth)
        # Table name suggests wrong vertices, but FKs correct it
        pk_columns = ["customer_id", "item_id"]
        fk_columns = [
            {"column": "customer_id", "references_table": "customer"},
            {"column": "item_id", "references_table": "item"},
        ]
        source, target, relation = infer_edge_vertices_from_table_name(
            "wrong_user_wrong_product_purchase",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        # FKs should override table name matching
        assert source == "customer", (
            f"Expected source='customer' (from FK), got {source}"
        )
        assert target == "item", f"Expected target='item' (from FK), got {target}"
        # Relation should still be derived from table name (not "wrong" fragments)
        assert relation is not None, "Relation should be inferred from table name"
        assert "wrong" not in relation.lower(), (
            f"Relation should not contain 'wrong', got {relation}"
        )

    def test_matching_stop_conditions(self):
        """Test that matching stops correctly when target_index > source_index + 1."""
        vertex_names = ["user", "product"]
        pk_columns = ["user_id", "product_id"]
        fk_columns = []

        # Case where target comes before source in the name
        # This should stop early and not match incorrectly
        # Example: "product_user_something" - if we match product first (left),
        # then user (right), we should stop if they're too far apart
        source, target, _relation = infer_edge_vertices_from_table_name(
            "product_something_else_user",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        # Should still match, but relation should be "something_else" or parts of it
        # The logic should handle this case gracefully
        assert source is not None, "Should find source"
        assert target is not None, "Should find target"

    def test_relation_scoring_and_flexible_patterns(self):
        """Test the new scoring system and flexible relation patterns.

        Tests that:
        - Relations can appear before, between, or after source/target
        - Scoring prefers longer fragments further to the right
        - Fragments < 3 chars don't get position bonus
        """
        vertex_names = ["user", "product", "order"]
        pk_columns = ["user_id", "product_id"]
        fk_columns = []

        # Pattern: bla_<relation>_SOURCE_TARGET
        # Example: "rel_purchase_user_product" -> user, product, purchase
        # "purchase" should be selected even though it's before source/target
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_purchase_user_product",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user"
        assert target == "product"
        assert relation == "purchase", f"Expected relation='purchase', got {relation}"

        # Pattern: SOURCE_<relation1>_<relation2>_TARGET
        # Example: "user_has_many_products" -> user, product
        # Should prefer longer fragment further right: "many" (score: 4 + 2*5 = 14)
        # over "has" (score: 3 + 1*5 = 8)
        pk_columns = ["user_id", "product_id"]
        source, target, relation = infer_edge_vertices_from_table_name(
            "user_has_many_products",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user"
        assert target == "product"
        # "many" should be selected (longer and further right)
        assert relation == "many", f"Expected relation='many', got {relation}"

        # Pattern: SOURCE_TARGET_<relation1>_<relation2>
        # Example: "user_product_purchase_history" -> user, product
        # Should prefer "history" (score: 7 + 3*5 = 22) over "purchase" (score: 8 + 2*5 = 18)
        source, target, relation = infer_edge_vertices_from_table_name(
            "user_product_purchase_history",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user"
        assert target == "product"
        # "history" should be selected (further right, even though "purchase" is longer)
        assert relation == "history", f"Expected relation='history', got {relation}"

        # Test that fragments < 3 chars don't get position bonus
        # Example: "user_id_product" -> user, product
        # "id" (length 2) should not get position bonus, so it won't be selected
        # if there are longer candidates
        pk_columns = ["user_id", "product_id"]
        source, target, relation = infer_edge_vertices_from_table_name(
            "rel_user_id_product",
            pk_columns,
            fk_columns,
            vertex_names,
        )
        assert source == "user"
        assert target == "product"
        # "id" should not be selected (too short, no position bonus)
        # "rel" should be selected if it's the only candidate, or nothing if filtered out
        assert relation is not None


class TestHelperFunctions:
    """Test suite for helper functions extracted from infer_edge_vertices_from_table_name."""

    def test_extract_key_fragments(self):
        """Test extraction of key fragments from PK and FK columns."""
        pk_columns = ["plant_id", "machine_id"]
        fk_columns = [
            {"column": "source_plant_id", "references_table": "plant"},
            {"column": "target_machine_id", "references_table": "machine"},
        ]
        separator = "_"

        fragments = _extract_key_fragments(pk_columns, fk_columns, separator)

        # Should extract unique fragments in order (PK first, then FK)
        assert "plant" in fragments
        assert "machine" in fragments
        assert "id" in fragments
        assert "source" in fragments
        assert "target" in fragments
        # Check order: PK fragments come first
        assert fragments.index("plant") < fragments.index("source")

    def test_match_vertices_from_table_fragments(self):
        """Test matching vertices from table name fragments."""
        table_fragments = ["rel", "user", "purchases", "product"]
        vertex_names = ["user", "product"]
        matcher = FuzzyMatcher(vertex_names, threshold=0.6, enable_cache=True)

        (
            source_idx,
            target_idx,
            source_vertex,
            target_vertex,
            matched_set,
        ) = _match_vertices_from_table_fragments(table_fragments, matcher)

        # Should match source from left (user at index 1)
        assert source_idx == 1
        assert source_vertex == "user"
        # Should match target from right (product at index 3)
        assert target_idx == 3
        assert target_vertex == "product"
        # Should track matched vertices
        assert "user" in matched_set
        assert "product" in matched_set

    def test_match_vertices_from_table_fragments_self_reference(self):
        """Test matching vertices when source and target are the same."""
        table_fragments = ["user", "follows", "user"]
        vertex_names = ["user"]
        matcher = FuzzyMatcher(vertex_names, threshold=0.6, enable_cache=True)

        (
            source_idx,
            target_idx,
            source_vertex,
            target_vertex,
            matched_set,
        ) = _match_vertices_from_table_fragments(table_fragments, matcher)

        # Should match source from left
        assert source_idx == 0
        assert source_vertex == "user"
        # Should match target from right (same vertex)
        assert target_idx == 2
        assert target_vertex == "user"
        assert matched_set == {"user"}

    def test_match_vertices_from_key_fragments(self):
        """Test matching vertices from key fragments."""
        key_fragments = ["plant", "machine", "id"]
        vertex_names = ["plant", "machine"]
        matcher = FuzzyMatcher(vertex_names, threshold=0.6, enable_cache=True)
        matched_set = set()

        matched_vertices, key_matched = _match_vertices_from_key_fragments(
            key_fragments, matcher, matched_set, None, None
        )

        # Should match plant and machine
        assert "plant" in matched_vertices
        assert "machine" in matched_vertices
        assert "plant" in key_matched
        assert "machine" in key_matched
        # Should not include "id" (not a vertex)
        assert "id" not in matched_vertices

    def test_match_vertices_from_key_fragments_with_existing(self):
        """Test matching when some vertices already matched from table name."""
        key_fragments = ["plant", "machine"]
        vertex_names = ["plant", "machine", "user"]
        matcher = FuzzyMatcher(vertex_names, threshold=0.6, enable_cache=True)
        matched_set = {"user"}  # Already matched from table name

        matched_vertices, key_matched = _match_vertices_from_key_fragments(
            key_fragments, matcher, matched_set, "user", None
        )

        # Should include user (from table name) and plant/machine (from keys)
        assert "user" in matched_vertices
        assert "plant" in matched_vertices
        assert "machine" in matched_vertices
        # Key matched should only include plant and machine
        assert "plant" in key_matched
        assert "machine" in key_matched
        assert "user" not in key_matched

    def test_extract_fk_vertex_names(self):
        """Test extraction of vertex names from foreign keys."""
        fk_columns = [
            {"column": "plant_id", "references_table": "plant"},
            {"column": "machine_id", "references_table": "machine"},
        ]

        fk_vertex_names = _extract_fk_vertex_names(fk_columns)

        assert fk_vertex_names == ["plant", "machine"]

    def test_extract_fk_vertex_names_with_none(self):
        """Test extraction when some FKs don't have references_table."""
        fk_columns = [
            {"column": "plant_id", "references_table": "plant"},
            {"column": "machine_id", "references_table": None},
        ]

        fk_vertex_names = _extract_fk_vertex_names(fk_columns)

        assert fk_vertex_names == ["plant"]

    def test_determine_source_target_vertices_priority_fk(self):
        """Test that FK references take highest priority."""
        fk_vertex_names = ["plant", "machine"]
        source_idx, target_idx = None, None
        source_vertex, target_vertex = None, None
        key_matched = ["wrong", "also_wrong"]
        matched_vertices = ["wrong", "also_wrong"]

        source, target = _determine_source_target_vertices(
            fk_vertex_names,
            source_idx,
            target_idx,
            source_vertex,
            target_vertex,
            key_matched,
            matched_vertices,
        )

        # FK should override fuzzy matches
        assert source == "plant"
        assert target == "machine"

    def test_determine_source_target_vertices_priority_table_name(self):
        """Test that table name matches take priority over key matches."""
        fk_vertex_names = []
        source_idx, target_idx = 1, 3
        source_vertex, target_vertex = "user", "product"
        key_matched = ["plant", "machine"]
        matched_vertices = ["user", "product"]

        source, target = _determine_source_target_vertices(
            fk_vertex_names,
            source_idx,
            target_idx,
            source_vertex,
            target_vertex,
            key_matched,
            matched_vertices,
        )

        # Table name matches should be used
        assert source == "user"
        assert target == "product"

    def test_determine_source_target_vertices_priority_key_matched(self):
        """Test that key-matched vertices take priority over other matches."""
        fk_vertex_names = []
        source_idx, target_idx = None, None
        source_vertex, target_vertex = None, None
        key_matched = ["plant", "machine"]
        matched_vertices = ["plant", "machine", "user"]

        source, target = _determine_source_target_vertices(
            fk_vertex_names,
            source_idx,
            target_idx,
            source_vertex,
            target_vertex,
            key_matched,
            matched_vertices,
        )

        # Key-matched vertices should be used
        assert source == "plant"
        assert target == "machine"

    def test_determine_source_target_vertices_self_reference(self):
        """Test self-reference case."""
        fk_vertex_names = ["user"]
        source_idx, target_idx = None, None
        source_vertex, target_vertex = None, None
        key_matched = []
        matched_vertices = ["user"]

        source, target = _determine_source_target_vertices(
            fk_vertex_names,
            source_idx,
            target_idx,
            source_vertex,
            target_vertex,
            key_matched,
            matched_vertices,
        )

        # Should be self-reference
        assert source == "user"
        assert target == "user"

    def test_identify_relation_name(self):
        """Test relation name identification from table fragments."""
        table_fragments = ["rel", "user", "purchases", "product"]
        source_idx, target_idx = 1, 3
        source_table, target_table = "user", "product"

        relation = _identify_relation_name(
            table_fragments, source_idx, target_idx, source_table, target_table
        )

        # Should identify "purchases" as relation (between user and product)
        assert relation == "purchases"

    def test_identify_relation_name_scoring(self):
        """Test that longer fragments further right are preferred."""
        table_fragments = ["user", "has", "many", "products"]
        source_idx, target_idx = 0, 3
        source_table, target_table = "user", "product"

        relation = _identify_relation_name(
            table_fragments, source_idx, target_idx, source_table, target_table
        )

        # "many" (length 4, index 2) should be preferred over "has" (length 3, index 1)
        # Score: "many" = 4 + 2*5 = 14, "has" = 3 + 1*5 = 8
        assert relation == "many"

    def test_identify_relation_name_no_candidates(self):
        """Test when no relation candidates are found."""
        table_fragments = ["user", "product"]
        source_idx, target_idx = 0, 1
        source_table, target_table = "user", "product"

        relation = _identify_relation_name(
            table_fragments, source_idx, target_idx, source_table, target_table
        )

        # Should return None when no relation candidates
        assert relation is None

    def test_identify_relation_name_fallback(self):
        """Test fallback logic when fragments don't match source/target."""
        table_fragments = ["prefix", "user", "product", "relation"]
        source_idx, target_idx = 1, 2
        source_table, target_table = "user", "product"

        relation = _identify_relation_name(
            table_fragments, source_idx, target_idx, source_table, target_table
        )

        # Should use fallback to find fragment that doesn't match source/target
        assert relation in ["prefix", "relation"]
