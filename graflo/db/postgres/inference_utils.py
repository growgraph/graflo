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


def _extract_key_fragments(
    pk_columns: list[str],
    fk_columns: list[dict[str, Any]],
    separator: str,
) -> list[str]:
    """Extract unique fragments from primary and foreign key column names.

    Args:
        pk_columns: List of primary key column names
        fk_columns: List of foreign key dictionaries with 'column' key
        separator: Separator character used to split column names

    Returns:
        List of unique fragments in order (PK fragments first, then FK fragments)
    """
    key_fragments_list: list[str] = []  # Preserve order
    key_fragments_set: set[str] = set()  # For deduplication

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

    return key_fragments_list


def _match_vertices_from_table_fragments(
    table_fragments: list[str],
    match_cache: FuzzyMatchCache,
) -> tuple[int | None, int | None, str | None, str | None, set[str]]:
    """Match vertices from table name fragments using left-to-right and right-to-left strategy.

    Args:
        table_fragments: List of fragments from table name
        match_cache: Fuzzy match cache for vertex matching

    Returns:
        Tuple of (source_match_idx, target_match_idx, source_vertex, target_vertex, matched_vertices_set)
    """
    source_match_idx: int | None = None
    target_match_idx: int | None = None
    source_vertex: str | None = None
    target_vertex: str | None = None
    matched_vertices_set: set[str] = set()

    # Match source starting from the left
    for i, fragment in enumerate(table_fragments):
        matched = match_cache.get_match(fragment)
        if matched and matched not in matched_vertices_set:
            source_match_idx = i
            source_vertex = matched
            matched_vertices_set.add(matched)
            break  # Found source, stop searching left

    # Match target starting from the right
    for i in range(
        len(table_fragments) - 1,
        source_match_idx if source_match_idx is not None else -1,
        -1,
    ):
        fragment = table_fragments[i]
        matched = match_cache.get_match(fragment)
        if matched:
            target_match_idx = i
            target_vertex = matched
            matched_vertices_set.add(matched)
            break  # Found target, stop searching right

    return (
        source_match_idx,
        target_match_idx,
        source_vertex,
        target_vertex,
        matched_vertices_set,
    )


def _match_vertices_from_key_fragments(
    key_fragments: list[str],
    match_cache: FuzzyMatchCache,
    matched_vertices_set: set[str],
    source_vertex: str | None,
    target_vertex: str | None,
) -> tuple[list[str], list[str]]:
    """Match vertices from key fragments and collect all matched vertices.

    Args:
        key_fragments: List of fragments extracted from key columns
        match_cache: Fuzzy match cache for vertex matching
        matched_vertices_set: Set of already matched vertices (will be updated)
        source_vertex: Source vertex matched from table name (if any)
        target_vertex: Target vertex matched from table name (if any)

    Returns:
        Tuple of (all_matched_vertices, key_matched_vertices)
    """
    matched_vertices: list[str] = []
    key_matched_vertices: list[str] = []

    # Add table name matches first
    if source_vertex:
        matched_vertices.append(source_vertex)
    if target_vertex and target_vertex != source_vertex:
        matched_vertices.append(target_vertex)

    # Match key fragments
    for fragment in key_fragments:
        matched = match_cache.get_match(fragment)
        if matched:
            if matched not in matched_vertices_set:
                matched_vertices.append(matched)
                matched_vertices_set.add(matched)
            # Track key-matched vertices separately for priority
            if matched not in key_matched_vertices:
                key_matched_vertices.append(matched)

    return (matched_vertices, key_matched_vertices)


def _extract_fk_vertex_names(fk_columns: list[dict[str, Any]]) -> list[str]:
    """Extract vertex names from foreign key references.

    Args:
        fk_columns: List of foreign key dictionaries with 'references_table' key

    Returns:
        List of referenced table names
    """
    fk_vertex_names: list[str] = []
    for fk in fk_columns:
        ref_table = fk.get("references_table")
        if ref_table:
            fk_vertex_names.append(ref_table)
    return fk_vertex_names


def _determine_source_target_vertices(
    fk_vertex_names: list[str],
    source_match_idx: int | None,
    target_match_idx: int | None,
    source_vertex: str | None,
    target_vertex: str | None,
    key_matched_vertices: list[str],
    matched_vertices: list[str],
) -> tuple[str | None, str | None]:
    """Determine source and target vertices using priority-based logic.

    Priority order:
    1. FK references (most reliable)
    2. Table name matches with indices (more specific)
    3. Key-matched vertices
    4. All matched vertices

    Args:
        fk_vertex_names: Vertex names from foreign key references
        source_match_idx: Index of source match in table fragments
        target_match_idx: Index of target match in table fragments
        source_vertex: Source vertex matched from table name
        target_vertex: Target vertex matched from table name
        key_matched_vertices: Vertices matched from key fragments
        matched_vertices: All matched vertices

    Returns:
        Tuple of (source_table, target_table)
    """
    source_table: str | None = None
    target_table: str | None = None

    # Priority 1: Use FK references if available (most reliable)
    if len(fk_vertex_names) >= 2:
        source_table = fk_vertex_names[0]
        target_table = fk_vertex_names[1]
    elif len(fk_vertex_names) == 1:
        # Self-reference case
        source_table = fk_vertex_names[0]
        target_table = fk_vertex_names[0]

    # Priority 2: Use matched vertices from fuzzy matching
    # Prefer table name matches with indices (more specific) over key matches
    if not source_table or not target_table:
        # If we have both source and target matches from table name with indices, use those
        # (table name is more specific about the actual relationship)
        if source_match_idx is not None and target_match_idx is not None:
            source_table = source_vertex
            target_table = target_vertex
        # Otherwise, if we have vertices from keys, prefer those
        elif len(key_matched_vertices) >= 2:
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

    return (source_table, target_table)


def _identify_relation_name(
    table_fragments: list[str],
    source_match_idx: int | None,
    target_match_idx: int | None,
    source_table: str | None,
    target_table: str | None,
) -> str | None:
    """Identify relation name from table fragments that are not source or target vertices.

    Args:
        table_fragments: List of fragments from table name
        source_match_idx: Index of source match in table fragments
        target_match_idx: Index of target match in table fragments
        source_table: Source vertex name
        target_table: Target vertex name

    Returns:
        Relation name or None if cannot identify
    """
    if not source_table or not target_table:
        return None

    vertex_idxs = {x for x in [source_match_idx, target_match_idx] if x is not None}
    source_lower = source_table.lower()
    target_lower = target_table.lower()

    relation_candidates: list[tuple[int, int, str]] = []

    # Collect all fragments that are not source or target
    # Allow relation to appear anywhere: before, between, or after source/target
    for idx, fragment in enumerate(table_fragments):
        if idx in vertex_idxs:
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
        return relation_name

    # Fallback: if we have 2+ fragments and one doesn't match source/target, it might be the relation
    if len(table_fragments) >= 2:
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
                return fragment

    return None


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

    # Step 1: Detect separator and split table name
    separator = detect_separator(table_name)
    table_fragments = split_by_separator(table_name, separator)

    # Step 2: Extract fragments from keys
    key_fragments = _extract_key_fragments(pk_columns, fk_columns, separator)

    # Step 3: Match vertices from table name fragments
    (
        source_match_idx,
        target_match_idx,
        source_vertex,
        target_vertex,
        matched_vertices_set,
    ) = _match_vertices_from_table_fragments(table_fragments, match_cache)

    # Step 4: Match vertices from key fragments
    matched_vertices, key_matched_vertices = _match_vertices_from_key_fragments(
        key_fragments, match_cache, matched_vertices_set, source_vertex, target_vertex
    )

    # Step 5: Extract FK vertex names
    fk_vertex_names = _extract_fk_vertex_names(fk_columns)

    # Step 6: Determine source and target vertices
    source_table, target_table = _determine_source_target_vertices(
        fk_vertex_names,
        source_match_idx,
        target_match_idx,
        source_vertex,
        target_vertex,
        key_matched_vertices,
        matched_vertices,
    )

    # Step 7: Identify relation name
    relation_name = _identify_relation_name(
        table_fragments, source_match_idx, target_match_idx, source_table, target_table
    )

    return (source_table, target_table, relation_name)


def _match_fragments_excluding_suffixes(
    fragments: list[str],
    match_cache: FuzzyMatchCache,
    common_suffixes: set[str],
) -> str | None:
    """Match fragments to vertices, excluding common suffixes.

    Args:
        fragments: List of fragments to match
        match_cache: Fuzzy match cache for vertex matching
        common_suffixes: Set of common suffix strings to skip

    Returns:
        Matched vertex name or None
    """
    for fragment in fragments:
        fragment_lower = fragment.lower()
        # Skip common suffixes
        if fragment_lower in common_suffixes:
            continue

        matched = match_cache.get_match(fragment)
        if matched:
            return matched
    return None


def _try_match_without_suffix(
    fragments: list[str],
    separator: str,
    match_cache: FuzzyMatchCache,
    common_suffixes: set[str],
) -> str | None:
    """Try matching fragments after removing common suffix.

    Args:
        fragments: List of fragments
        separator: Separator character
        match_cache: Fuzzy match cache for vertex matching
        common_suffixes: Set of common suffix strings

    Returns:
        Matched vertex name or None
    """
    if len(fragments) > 1:
        last_fragment = fragments[-1].lower()
        if last_fragment in common_suffixes:
            # Try matching the remaining fragments
            remaining = separator.join(fragments[:-1])
            matched = match_cache.get_match(remaining)
            if matched:
                return matched
    return None


def _try_exact_match_with_suffix_removal(
    column_name: str,
    vertex_table_names: list[str],
    common_suffixes: set[str],
) -> str | None:
    """Try exact match after removing common suffixes (last resort).

    Args:
        column_name: Column name to match
        vertex_table_names: List of vertex table names
        common_suffixes: Set of common suffix strings

    Returns:
        Matched vertex name or None
    """
    column_lower = column_name.lower()
    for vertex_name in vertex_table_names:
        vertex_lower = vertex_name.lower()
        # Check if column name contains vertex name
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

    # Common suffixes to remove: id, fk, key, pk, ref
    common_suffixes = {"id", "fk", "key", "pk", "ref", "reference"}

    # Step 1: Try matching full column name first
    matched = match_cache.get_match(column_name)
    if matched:
        return matched

    # Step 2: Detect separator and split column name
    separator = detect_separator(column_name)
    fragments = split_by_separator(column_name, separator)

    if not fragments:
        return None

    # Step 3: Try matching fragments (excluding common suffixes)
    matched = _match_fragments_excluding_suffixes(
        fragments, match_cache, common_suffixes
    )
    if matched:
        return matched

    # Step 4: Try removing common suffix and matching again
    matched = _try_match_without_suffix(
        fragments, separator, match_cache, common_suffixes
    )
    if matched:
        return matched

    # Step 5: As last resort, try exact match against vertex names (case-insensitive)
    return _try_exact_match_with_suffix_removal(
        column_name, vertex_table_names, common_suffixes
    )
