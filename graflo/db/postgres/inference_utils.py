"""Inference utilities for PostgreSQL schema analysis.

This module provides utility functions for inferring relationships and patterns
from PostgreSQL table and column names using heuristics and fuzzy matching.
"""

from typing import Any

from .fuzzy_matcher import FuzzyMatchCache, FuzzyMatcher


def fuzzy_match_fragment(
    fragment: str, vertex_names: list[str], threshold: float = 0.6
) -> str | None:
    """Fuzzy match a fragment to vertex names.

    Backward-compatible wrapper function that uses the improved FuzzyMatcher.

    Args:
        fragment: Fragment to match
        vertex_names: List of vertex table names to match against
        threshold: Similarity threshold (0.0 to 1.0)

    Returns:
        Best matching vertex name or None if no match above threshold
    """
    matcher = FuzzyMatcher(vertex_names, threshold)
    match, _ = matcher.match(fragment)
    return match


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


def infer_edge_vertices_from_table_name(
    table_name: str,
    pk_columns: list[str],
    fk_columns: list[dict[str, Any]],
    vertex_table_names: list[str] | None = None,
    match_cache: FuzzyMatchCache | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Infer source and target vertex names from table name and structure.

    Uses fuzzy matching to identify vertex names in table name fragments and key names.
    Handles patterns like:
    - rel_cluster_containment_host -> cluster, host, containment
    - rel_cluster_containment_cluster_2 -> cluster, cluster, containment (self-reference)
    - user_follows_user -> user, user, follows (self-reference)
    - product_category_mapping -> product, category, mapping

    Args:
        table_name: Name of the table
        pk_columns: List of primary key column names
        fk_columns: List of foreign key dictionaries with 'column' and 'references_table' keys
        vertex_table_names: Optional list of known vertex table names for fuzzy matching
        match_cache: Optional pre-computed fuzzy match cache for better performance

    Returns:
        Tuple of (source_table, target_table, relation_name) or (None, None, None) if cannot infer
    """
    if vertex_table_names is None:
        vertex_table_names = []

    # Use cache if provided, otherwise create a temporary one
    if match_cache is None:
        match_cache = FuzzyMatchCache(vertex_table_names)

    # Step 1: Detect separator
    separator = detect_separator(table_name)

    # Step 2: Split table name by separator
    table_fragments = split_by_separator(table_name, separator)

    # Initialize relation_name - will be set if we identify a relation fragment
    relation_name = None

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

    # Step 4: Match table name fragments to vertices
    # Strategy: match source from left, target from right
    # Stop when we have 2 matches OR target_index > source_index + 1
    source_match_idx: int | None = None
    target_match_idx: int | None = None
    source_vertex: str | None = None
    target_vertex: str | None = None
    matched_vertices_set = set()  # For deduplication
    matched_fragment_indices = {}  # Track which fragment indices matched which vertices

    # Match source starting from the left
    for i, fragment in enumerate(table_fragments):
        matched = match_cache.get_match(fragment)
        if matched and matched not in matched_vertices_set:
            source_match_idx = i
            source_vertex = matched
            matched_vertices_set.add(matched)
            matched_fragment_indices[i] = matched
            break  # Found source, stop searching left

    # Match target starting from the right
    for i in range(
        len(table_fragments) - 1,
        source_match_idx if source_match_idx is not None else -1,
        -1,
    ):
        fragment = table_fragments[i]
        matched = match_cache.get_match(fragment)
        if matched and matched not in matched_vertices_set:
            target_match_idx = i
            target_vertex = matched
            matched_vertices_set.add(matched)
            matched_fragment_indices[i] = matched
            break  # Found target, stop searching right

    # Match key fragments to fill in missing vertices (keys are primary source of truth)
    matched_vertices = []
    key_matched_vertices = []  # Track vertices matched from keys (higher priority)

    if source_vertex:
        matched_vertices.append(source_vertex)
    if target_vertex and target_vertex != source_vertex:
        matched_vertices.append(target_vertex)

    for fragment in key_fragments_list:
        matched = match_cache.get_match(fragment)
        if matched:
            if matched not in matched_vertices_set:
                matched_vertices.append(matched)
                matched_vertices_set.add(matched)
            # Track key-matched vertices separately for priority
            if matched not in key_matched_vertices:
                key_matched_vertices.append(matched)

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
    # Prefer vertices from keys (primary source of truth) over table matches
    if not source_table or not target_table:
        # If we have vertices from keys, prefer those
        if len(key_matched_vertices) >= 2:
            source_table = key_matched_vertices[0]
            target_table = key_matched_vertices[1]
        elif len(key_matched_vertices) == 1:
            # Use key vertex for source, try to find target from all matched vertices
            source_table = key_matched_vertices[0]
            if len(matched_vertices) >= 2:
                # Find target that's not the source
                for v in matched_vertices:
                    if v != source_table:
                        target_table = v
                        break
                if not target_table:
                    target_table = source_table  # Self-reference
            else:
                target_table = source_table  # Self-reference
        elif len(matched_vertices) >= 2:
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

    # Step 7: Identify relation from table fragments (after we know source/target)
    # Relation is derived from table name fragments that are neither source nor target
    # Patterns: bla_SOURCE_<relation>_TARGET, bla_SOURCE_TARGET_<relation>,
    #           bla_<relation>_SOURCE_TARGET, and other combinations
    # We allow relation to appear anywhere except as source/target fragments
    if relation_name is None and source_table and target_table:
        source_lower = source_table.lower()
        target_lower = target_table.lower()

        # Use the matched fragment indices if available (more precise)
        # Otherwise, find by matching fragment text
        source_idx = source_match_idx
        target_idx = target_match_idx

        # If we don't have indices from matching, find them by text matching
        if source_idx is None or target_idx is None:
            for idx, fragment in enumerate(table_fragments):
                fragment_lower = fragment.lower()
                if source_idx is None and (
                    fragment_lower == source_lower
                    or source_lower in fragment_lower
                    or fragment_lower in source_lower
                ):
                    source_idx = idx
                if target_idx is None and (
                    fragment_lower == target_lower
                    or target_lower in fragment_lower
                    or fragment_lower in target_lower
                ):
                    target_idx = idx

        relation_candidates = []

        # Collect all fragments that are not source or target
        # Allow relation to appear anywhere: before, between, or after source/target
        for idx, fragment in enumerate(table_fragments):
            fragment_lower = fragment.lower()

            # Skip if it's a source or target fragment
            if (
                fragment_lower == source_lower
                or source_lower in fragment_lower
                or fragment_lower in source_lower
                or fragment_lower == target_lower
                or target_lower in fragment_lower
                or fragment_lower in target_lower
            ):
                continue

            # Include all non-source/target fragments as relation candidates
            relation_candidates.append((len(fragment), idx, fragment))

        # Select candidate using scoring system:
        # - Score = fragment_length + (position_index * 5) if fragment_length >= 3
        # - Score = fragment_length if fragment_length < 3
        # - Prefer candidates further to the right and longer
        if relation_candidates:

            def score_candidate(candidate: tuple[int, int, str]) -> int:
                fragment_length, position_idx, _ = candidate
                if fragment_length >= 3:
                    # Position bonus: each position to the right counts as 5 extra characters
                    return fragment_length + (position_idx * 5)
                else:
                    # Fragments below 3 symbols don't get position bonus
                    return fragment_length

            _, _, relation_name = max(relation_candidates, key=score_candidate)
        elif len(table_fragments) >= 2:
            # Fallback: if we have 2+ fragments and one doesn't match source/target, it might be the relation
            for fragment in table_fragments:
                fragment_lower = fragment.lower()
                # Use if it doesn't match source or target
                if (
                    fragment_lower != source_lower
                    and source_lower not in fragment_lower
                    and fragment_lower not in source_lower
                    and fragment_lower != target_lower
                    and target_lower not in fragment_lower
                    and fragment_lower not in target_lower
                ):
                    relation_name = fragment
                    break

    return (source_table, target_table, relation_name)


def infer_vertex_from_column_name(
    column_name: str,
    vertex_table_names: list[str] | None = None,
    match_cache: FuzzyMatchCache | None = None,
) -> str | None:
    """Infer vertex table name from a column name using robust pattern matching.

    Uses the same logic as infer_edge_vertices_from_table_name but focused on
    extracting vertex names from column names. Handles patterns like:
    - user_id -> user
    - product_id -> product
    - customer_fk -> customer
    - source_vertex -> source_vertex (if matches)

    Args:
        column_name: Name of the column
        vertex_table_names: Optional list of known vertex table names for fuzzy matching
        match_cache: Optional pre-computed fuzzy match cache for better performance

    Returns:
        Inferred vertex table name or None if cannot infer
    """
    if vertex_table_names is None:
        vertex_table_names = []

    # Use cache if provided, otherwise create a temporary one
    if match_cache is None:
        match_cache = FuzzyMatchCache(vertex_table_names)

    if not column_name:
        return None

    # Step 1: Detect separator
    separator = detect_separator(column_name)

    # Step 2: Split column name by separator
    fragments = split_by_separator(column_name, separator)

    if not fragments:
        return None

    # Step 3: Try to match fragments to vertex names
    # Common suffixes to remove: id, fk, key, pk, ref
    common_suffixes = {"id", "fk", "key", "pk", "ref", "reference"}

    # Try matching full column name first
    matched = match_cache.get_match(column_name)
    if matched:
        return matched

    # Try matching fragments (excluding common suffixes)
    for fragment in fragments:
        fragment_lower = fragment.lower()
        # Skip common suffixes
        if fragment_lower in common_suffixes:
            continue

        matched = match_cache.get_match(fragment)
        if matched:
            return matched

    # Step 4: If no match found, try removing common suffixes and matching again
    # Remove last fragment if it's a common suffix
    if len(fragments) > 1:
        last_fragment = fragments[-1].lower()
        if last_fragment in common_suffixes:
            # Try matching the remaining fragments
            remaining = separator.join(fragments[:-1])
            matched = match_cache.get_match(remaining)
            if matched:
                return matched

    # Step 5: As last resort, try exact match against vertex names (case-insensitive)
    column_lower = column_name.lower()
    for vertex_name in vertex_table_names:
        vertex_lower = vertex_name.lower()
        # Check if column name contains vertex name or vice versa
        if vertex_lower in column_lower:
            # Remove common suffixes from column name and check if it matches
            for suffix in common_suffixes:
                if column_lower.endswith(f"_{suffix}") or column_lower.endswith(suffix):
                    base = (
                        column_lower[: -len(f"_{suffix}")]
                        if column_lower.endswith(f"_{suffix}")
                        else column_lower[: -len(suffix)]
                    )
                    if base == vertex_lower:
                        return vertex_name

    return None
