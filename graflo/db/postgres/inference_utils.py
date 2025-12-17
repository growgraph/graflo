"""Inference utilities for PostgreSQL schema analysis.

This module provides utility functions for inferring relationships and patterns
from PostgreSQL table and column names using heuristics and fuzzy matching.
"""

from difflib import SequenceMatcher
from typing import Any


def detect_separator(text: str) -> str:
    """Detect the most common separator character in a text.

    Args:
        text: Text to analyze

    Returns:
        Most common separator character, defaults to '_'
    """
    # Common separators
    separators = ["_", "-", "."]
    counts = {sep: text.count(sep) for sep in separators}

    if max(counts.values()) > 0:
        return max(counts, key=counts.get)
    return "_"  # Default separator


def split_by_separator(text: str, separator: str) -> list[str]:
    """Split text by separator, handling multiple consecutive separators.

    Args:
        text: Text to split
        separator: Separator character

    Returns:
        List of non-empty fragments
    """
    # Split and filter out empty strings
    parts = [p for p in text.split(separator) if p]
    return parts


def fuzzy_match_fragment(
    fragment: str, vertex_names: list[str], threshold: float = 0.6
) -> str | None:
    """Fuzzy match a fragment to vertex names.

    Args:
        fragment: Fragment to match
        vertex_names: List of vertex table names to match against
        threshold: Similarity threshold (0.0 to 1.0)

    Returns:
        Best matching vertex name or None if no match above threshold
    """
    if not vertex_names:
        return None

    fragment_lower = fragment.lower()
    best_match = None
    best_score = 0.0

    for vertex_name in vertex_names:
        vertex_lower = vertex_name.lower()

        # Exact match (case-insensitive)
        if fragment_lower == vertex_lower:
            return vertex_name

        # Check if fragment is contained in vertex name or vice versa
        if fragment_lower in vertex_lower or vertex_lower in fragment_lower:
            score = min(len(fragment_lower), len(vertex_lower)) / max(
                len(fragment_lower), len(vertex_lower)
            )
            if score > best_score:
                best_score = score
                best_match = vertex_name

        # Use SequenceMatcher for fuzzy matching
        similarity = SequenceMatcher(None, fragment_lower, vertex_lower).ratio()
        if similarity > best_score:
            best_score = similarity
            best_match = vertex_name

    return best_match if best_score >= threshold else None


def is_relation_fragment(fragment: str) -> bool:
    """Check if a fragment looks like a relation name rather than a vertex name.

    Only very short fragments (<= 2 chars) are considered relations by default.
    Actual relation identification is done contextually in infer_edge_vertices_from_table_name
    based on fragment position and length when there are more than 2 fragments.

    Args:
        fragment: Fragment to check

    Returns:
        True if fragment is very short (likely not a vertex name)
    """
    # Only very short fragments are considered relations by default
    # Longer relation identification is done contextually based on position
    if len(fragment) <= 2:
        return True

    return False


def infer_edge_vertices_from_table_name(
    table_name: str,
    pk_columns: list[str],
    fk_columns: list[dict[str, Any]],
    vertex_table_names: list[str] | None = None,
) -> tuple[str | None, str | None]:
    """Infer source and target vertex names from table name and structure.

    Uses fuzzy matching to identify vertex names in table name fragments and key names.
    Handles patterns like:
    - rel_cluster_containment_host -> cluster, host
    - rel_cluster_containment_cluster_2 -> cluster, cluster (self-reference)
    - user_follows_user -> user, user (self-reference)
    - product_category_mapping -> product, category

    Args:
        table_name: Name of the table
        pk_columns: List of primary key column names
        fk_columns: List of foreign key dictionaries with 'column' and 'references_table' keys
        vertex_table_names: Optional list of known vertex table names for fuzzy matching

    Returns:
        Tuple of (source_table, target_table) or (None, None) if cannot infer
    """
    if vertex_table_names is None:
        vertex_table_names = []

    # Step 1: Detect separator
    separator = detect_separator(table_name)

    # Step 2: Split table name by separator
    table_fragments = split_by_separator(table_name, separator)

    # Step 3: Extract fragments from keys (preserve order for PK columns)
    key_fragments_list = []  # Preserve order
    key_fragments_set = set()  # For deduplication

    # Extract fragments from PK columns in order
    for pk_col in pk_columns:
        pk_fragments = split_by_separator(pk_col, separator)
        for frag in pk_fragments:
            if frag not in key_fragments_set:
                key_fragments_list.append(frag)
                key_fragments_set.add(frag)

    # Extract fragments from FK columns
    for fk in fk_columns:
        fk_col = fk.get("column", "")
        fk_fragments = split_by_separator(fk_col, separator)
        for frag in fk_fragments:
            if frag not in key_fragments_set:
                key_fragments_list.append(frag)
                key_fragments_set.add(frag)

    # Step 4: Fuzzy match fragments to vertex names
    matched_vertices = []  # Preserve order - first match is source, second is target
    matched_vertices_set = set()  # For deduplication
    matched_fragment_indices = {}  # Track which fragment indices matched which vertices

    # Match table name fragments first (higher priority, preserves order)
    # Skip very short fragments (likely not vertices)
    for i, fragment in enumerate(table_fragments):
        if is_relation_fragment(fragment):
            continue

        matched = fuzzy_match_fragment(fragment, vertex_table_names)
        if matched and matched not in matched_vertices_set:
            matched_vertices.append(matched)
            matched_vertices_set.add(matched)
            matched_fragment_indices[i] = matched

    # If we have more than 2 fragments and have matched 2 vertices, identify relation fragment
    # and skip it in subsequent matching
    relation_fragment_indices = set()
    if len(table_fragments) > 2 and len(matched_vertices) >= 2:
        source_idx = None
        target_idx = None
        for idx, vertex in matched_fragment_indices.items():
            if vertex == matched_vertices[0] and source_idx is None:
                source_idx = idx
            if vertex == matched_vertices[1] and target_idx is None:
                target_idx = idx

        if source_idx is not None and target_idx is not None:
            # Ensure source comes before target
            if source_idx > target_idx:
                source_idx, target_idx = target_idx, source_idx

            # Find longest fragment between source and target, or after both
            relation_candidates = []
            for idx, fragment in enumerate(table_fragments):
                if idx in matched_fragment_indices or is_relation_fragment(fragment):
                    continue
                if (source_idx < idx < target_idx) or (idx > target_idx):
                    relation_candidates.append((len(fragment), idx, fragment))

            if relation_candidates:
                _, relation_fragment_idx, relation_fragment = max(
                    relation_candidates, key=lambda x: x[0]
                )
                relation_fragment_indices.add(relation_fragment_idx)

    # Match key fragments (add to end if not already matched)
    # Skip relation fragments identified above
    for fragment in key_fragments_list:
        if is_relation_fragment(fragment):
            continue

        matched = fuzzy_match_fragment(fragment, vertex_table_names)
        if matched and matched not in matched_vertices_set:
            matched_vertices.append(matched)
            matched_vertices_set.add(matched)

    # Step 5: Use foreign keys to confirm or infer vertices
    fk_vertex_names = []
    if fk_columns:
        for fk in fk_columns:
            ref_table = fk.get("references_table")
            if ref_table:
                fk_vertex_names.append(ref_table)

    # Step 6: Form hypothesis
    source_table = None
    target_table = None

    # Priority 1: Use FK references if available (most reliable)
    if len(fk_vertex_names) >= 2:
        source_table = fk_vertex_names[0]
        target_table = fk_vertex_names[1]
    elif len(fk_vertex_names) == 1:
        # Self-reference case
        source_table = fk_vertex_names[0]
        target_table = fk_vertex_names[0]

    # Priority 2: Use matched vertices from fuzzy matching
    if not source_table or not target_table:
        if len(matched_vertices) >= 2:
            source_table = matched_vertices[0]
            target_table = matched_vertices[1]
        elif len(matched_vertices) == 1:
            # Self-reference case
            source_table = matched_vertices[0]
            target_table = matched_vertices[0]

    # Priority 3: Fill in missing vertex from remaining options
    if source_table and not target_table:
        # Try to find target from remaining fragments or keys
        if fk_vertex_names and len(fk_vertex_names) > 1:
            # Use second FK if available
            target_table = fk_vertex_names[1]
        elif matched_vertices and len(matched_vertices) > 1:
            target_table = matched_vertices[1]
        elif fk_vertex_names:
            # Self-reference case
            target_table = fk_vertex_names[0]
        elif matched_vertices:
            target_table = matched_vertices[0]

    if target_table and not source_table:
        # Try to find source from remaining fragments or keys
        if fk_vertex_names:
            source_table = fk_vertex_names[0]
        elif matched_vertices:
            source_table = matched_vertices[0]

    return (source_table, target_table)
