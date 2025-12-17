"""Tests for relation table identification logic in PostgreSQL connection."""

from graflo.db.postgres.inference_utils import (
    detect_separator,
    fuzzy_match_fragment,
    infer_edge_vertices_from_table_name,
    is_relation_fragment,
    split_by_separator,
)


class TestRelationIdentification:
    """Test suite for relation table identification logic."""

    def test_detect_separator(self):
        """Test separator detection."""
        # Test underscore separator
        assert detect_separator("rel_cluster_containment_host") == "_"

        # Test hyphen separator
        assert detect_separator("rel-cluster-containment-host") == "-"

        # Test dot separator
        assert detect_separator("rel.cluster.containment.host") == "."

        # Test default when no separator
        assert detect_separator("relclustercontainmenthost") == "_"

        # Test mixed separators (should pick most common)
        assert detect_separator("rel_cluster-containment_host") == "_"

    def test_split_by_separator(self):
        """Test splitting by separator."""
        # Test underscore separator
        assert split_by_separator("rel_cluster_containment_host", "_") == [
            "rel",
            "cluster",
            "containment",
            "host",
        ]

        # Test hyphen separator
        assert split_by_separator("rel-cluster-containment-host", "-") == [
            "rel",
            "cluster",
            "containment",
            "host",
        ]

        # Test multiple consecutive separators
        assert split_by_separator("rel__cluster___containment", "_") == [
            "rel",
            "cluster",
            "containment",
        ]

        # Test empty string
        assert split_by_separator("", "_") == []

    def test_fuzzy_match_fragment(self):
        """Test fuzzy matching of fragments to vertex names."""
        vertex_names = ["cluster", "host", "user", "product", "category"]

        # Test exact match (case-insensitive)
        assert fuzzy_match_fragment("cluster", vertex_names) == "cluster"
        assert fuzzy_match_fragment("CLUSTER", vertex_names) == "cluster"

        # Test substring match
        assert fuzzy_match_fragment("clust", vertex_names) == "cluster"
        assert fuzzy_match_fragment("hosts", vertex_names) == "host"

        # Test fuzzy match
        assert fuzzy_match_fragment("usr", vertex_names) == "user"
        assert fuzzy_match_fragment("prod", vertex_names) == "product"

        # Test no match (below threshold)
        assert fuzzy_match_fragment("xyz", vertex_names) is None

        # Test empty vertex names
        assert fuzzy_match_fragment("cluster", []) is None

    def test_is_relation_fragment(self):
        """Test relation fragment detection."""
        # Test relation keywords
        assert is_relation_fragment("rel") is True
        assert is_relation_fragment("contains") is True
        assert is_relation_fragment("has") is True
        assert is_relation_fragment("belongs") is True
        assert is_relation_fragment("references") is True

        # Test short fragments
        assert is_relation_fragment("ab") is True
        assert is_relation_fragment("a") is True

        # Test vertex-like fragments
        assert is_relation_fragment("cluster") is False
        assert is_relation_fragment("user") is False
        assert is_relation_fragment("product") is False

    def test_infer_edge_vertices_from_table_name_with_fks(self):
        """Test inference when foreign keys are available."""
        pk_columns = ["cluster_id", "host_id"]
        fk_columns = [
            {"column": "cluster_id", "references_table": "cluster"},
            {"column": "host_id", "references_table": "host"},
        ]
        vertex_names = ["cluster", "host", "user"]

        # Test with FK references (most reliable)
        source, target = infer_edge_vertices_from_table_name(
            "rel_cluster_containment_host", pk_columns, fk_columns, vertex_names
        )
        assert source == "cluster"
        assert target == "host"

        # Test self-reference
        fk_columns_self = [{"column": "user_id", "references_table": "user"}]
        source, target = infer_edge_vertices_from_table_name(
            "rel_user_follows_user",
            ["user_id", "follows_user_id"],
            fk_columns_self,
            vertex_names,
        )
        assert source == "user"
        assert target == "user"

    def test_infer_edge_vertices_from_table_name_fuzzy_matching(self):
        """Test inference using fuzzy matching when FKs are not available."""
        pk_columns = ["cluster_id", "host_id"]
        fk_columns = []
        vertex_names = ["cluster", "host", "user", "product", "category"]

        # Test fuzzy matching from table name
        source, target = infer_edge_vertices_from_table_name(
            "rel_cluster_containment_host", pk_columns, fk_columns, vertex_names
        )
        assert source == "cluster"
        assert target == "host"

        # Test with different separator
        source, target = infer_edge_vertices_from_table_name(
            "rel-cluster-containment-host", pk_columns, fk_columns, vertex_names
        )
        assert source == "cluster"
        assert target == "host"

        # Test product_category_mapping pattern
        source, target = infer_edge_vertices_from_table_name(
            "product_category_mapping",
            ["product_id", "category_id"],
            fk_columns,
            vertex_names,
        )
        assert source == "product"
        assert target == "category"

    def test_infer_edge_vertices_from_key_fragments(self):
        """Test inference from key column fragments."""
        pk_columns = ["cluster_id", "host_id"]
        fk_columns = []
        vertex_names = ["cluster", "host"]

        # Test matching from PK column names
        source, target = infer_edge_vertices_from_table_name(
            "rel_containment", pk_columns, fk_columns, vertex_names
        )
        assert source == "cluster"
        assert target == "host"

        # Test with FK column fragments
        fk_columns_with_fragments = [
            {"column": "source_cluster_id", "references_table": None},
            {"column": "target_host_id", "references_table": None},
        ]
        source, target = infer_edge_vertices_from_table_name(
            "rel_containment", pk_columns, fk_columns_with_fragments, vertex_names
        )
        assert source == "cluster"
        assert target == "host"

    def test_infer_edge_vertices_priority(self):
        """Test that FK references take priority over fuzzy matching."""
        pk_columns = ["wrong_id", "also_wrong_id"]
        fk_columns = [
            {"column": "cluster_id", "references_table": "cluster"},
            {"column": "host_id", "references_table": "host"},
        ]
        vertex_names = ["cluster", "host", "wrong", "also_wrong"]

        # FK references should override fuzzy matches from table/column names
        source, target = infer_edge_vertices_from_table_name(
            "rel_wrong_also_wrong", pk_columns, fk_columns, vertex_names
        )
        assert source == "cluster"
        assert target == "host"

    def test_infer_edge_vertices_no_match(self):
        """Test inference when no matches are found."""
        pk_columns = ["xyz_id", "abc_id"]
        fk_columns = []
        vertex_names = ["cluster", "host"]

        source, target = infer_edge_vertices_from_table_name(
            "rel_xyz_abc", pk_columns, fk_columns, vertex_names
        )
        assert source is None
        assert target is None

        # Test with empty vertex names
        source, target = infer_edge_vertices_from_table_name(
            "rel_cluster_host", pk_columns, fk_columns, []
        )
        assert source is None
        assert target is None

    def test_infer_edge_vertices_complex_patterns(self):
        """Test inference with complex naming patterns."""
        vertex_names = ["cluster", "host", "user", "product", "category"]

        # Test pattern: rel_<source>_<relation>_<target>_<number>
        pk_columns = ["cluster_id", "cluster_id_2"]
        fk_columns = [
            {"column": "cluster_id", "references_table": "cluster"},
            {"column": "cluster_id_2", "references_table": "cluster"},
        ]
        source, target = infer_edge_vertices_from_table_name(
            "rel_cluster_containment_cluster_2", pk_columns, fk_columns, vertex_names
        )
        assert source == "cluster"
        assert target == "cluster"

        # Test pattern without rel_ prefix
        pk_columns = ["user_id", "follows_user_id"]
        fk_columns = []
        source, target = infer_edge_vertices_from_table_name(
            "user_follows_user", pk_columns, fk_columns, vertex_names
        )
        assert source == "user"
        assert target == "user"
