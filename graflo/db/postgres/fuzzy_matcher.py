"""Fuzzy matching utilities for PostgreSQL schema analysis.

This module provides improved fuzzy matching strategies for identifying
vertex names from table and column fragments.
"""

from difflib import SequenceMatcher


class FuzzyMatcher:
    """Improved fuzzy matcher with multiple matching strategies.

    Uses a combination of matching techniques:
    1. Exact matching (case-insensitive)
    2. Substring matching (with length-based scoring)
    3. Sequence similarity (difflib)
    4. Prefix/suffix matching
    5. Common pattern matching (handles id, fk, etc.)
    """

    def __init__(self, vertex_names: list[str], threshold: float = 0.6):
        """Initialize the fuzzy matcher.

        Args:
            vertex_names: List of vertex table names to match against
            threshold: Similarity threshold (0.0 to 1.0)
        """
        self.vertex_names = vertex_names
        self.threshold = threshold
        # Pre-compute lowercase versions for efficiency
        self._vertex_lower_map = {vn.lower(): vn for vn in vertex_names}
        self._vertex_lower_list = list(self._vertex_lower_map.keys())

    def match(self, fragment: str) -> tuple[str | None, float]:
        """Match a fragment against vertex names using multiple strategies.

        Args:
            fragment: Fragment to match

        Returns:
            Tuple of (best_match, score) or (None, 0.0) if no match above threshold
        """
        if not self.vertex_names or not fragment:
            return (None, 0.0)

        fragment_lower = fragment.lower()

        # Strategy 1: Exact match (highest priority, returns immediately)
        if fragment_lower in self._vertex_lower_map:
            return (self._vertex_lower_map[fragment_lower], 1.0)

        best_match = None
        best_score = 0.0

        # Strategy 2: Substring matching with length-based scoring
        substring_score = self._substring_match(fragment_lower)
        if substring_score[1] > best_score:
            best_match, best_score = substring_score

        # Strategy 3: Sequence similarity (difflib)
        sequence_score = self._sequence_match(fragment_lower)
        if sequence_score[1] > best_score:
            best_match, best_score = sequence_score

        # Strategy 4: Prefix/suffix matching
        prefix_suffix_score = self._prefix_suffix_match(fragment_lower)
        if prefix_suffix_score[1] > best_score:
            best_match, best_score = prefix_suffix_score

        # Strategy 5: Common pattern matching (handles id, fk, etc.)
        pattern_score = self._pattern_match(fragment_lower)
        if pattern_score[1] > best_score:
            best_match, best_score = pattern_score

        # Return match only if above threshold
        if best_score >= self.threshold:
            return (best_match, best_score)
        return (None, 0.0)

    def _substring_match(self, fragment_lower: str) -> tuple[str | None, float]:
        """Match using substring containment with length-based scoring.

        Args:
            fragment_lower: Lowercase fragment to match

        Returns:
            Tuple of (best_match, score)
        """
        best_match = None
        best_score = 0.0

        for vertex_lower, vertex_name in self._vertex_lower_map.items():
            # Check if fragment is contained in vertex or vice versa
            if fragment_lower in vertex_lower:
                # Fragment is substring of vertex (e.g., "user" in "users")
                score = len(fragment_lower) / len(vertex_lower)
                # Boost score if fragment is significant portion
                if len(fragment_lower) >= 3:  # At least 3 chars
                    score = min(score * 1.2, 0.95)  # Cap at 0.95
                if score > best_score:
                    best_score = score
                    best_match = vertex_name
            elif vertex_lower in fragment_lower:
                # Vertex is substring of fragment (e.g., "user" in "user_id")
                score = len(vertex_lower) / len(fragment_lower)
                # Boost score if vertex is significant portion
                if len(vertex_lower) >= 3:
                    score = min(score * 1.2, 0.95)
                if score > best_score:
                    best_score = score
                    best_match = vertex_name

        return (best_match, best_score)

    def _sequence_match(self, fragment_lower: str) -> tuple[str | None, float]:
        """Match using sequence similarity (difflib).

        Args:
            fragment_lower: Lowercase fragment to match

        Returns:
            Tuple of (best_match, score)
        """
        best_match = None
        best_score = 0.0

        for vertex_lower, vertex_name in self._vertex_lower_map.items():
            similarity = SequenceMatcher(None, fragment_lower, vertex_lower).ratio()
            if similarity > best_score:
                best_score = similarity
                best_match = vertex_name

        return (best_match, best_score)

    def _prefix_suffix_match(self, fragment_lower: str) -> tuple[str | None, float]:
        """Match using prefix or suffix patterns.

        Args:
            fragment_lower: Lowercase fragment to match

        Returns:
            Tuple of (best_match, score)
        """
        best_match = None
        best_score = 0.0

        for vertex_lower, vertex_name in self._vertex_lower_map.items():
            # Check prefix match
            if fragment_lower.startswith(vertex_lower):
                score = len(vertex_lower) / len(fragment_lower)
                if score > best_score:
                    best_score = score
                    best_match = vertex_name
            # Check suffix match
            elif fragment_lower.endswith(vertex_lower):
                score = len(vertex_lower) / len(fragment_lower)
                if score > best_score:
                    best_score = score
                    best_match = vertex_name
            # Check if vertex starts with fragment
            elif vertex_lower.startswith(fragment_lower):
                score = len(fragment_lower) / len(vertex_lower)
                if score > best_score:
                    best_score = score
                    best_match = vertex_name

        return (best_match, best_score)

    def _pattern_match(self, fragment_lower: str) -> tuple[str | None, float]:
        """Match using common patterns (id, fk, etc.).

        Args:
            fragment_lower: Lowercase fragment to match

        Returns:
            Tuple of (best_match, score)
        """
        # Common suffixes/prefixes to remove
        common_patterns = [
            ("_id", ""),
            ("_fk", ""),
            ("_key", ""),
            ("_pk", ""),
            ("_ref", ""),
            ("_reference", ""),
            ("id_", ""),
            ("fk_", ""),
            ("key_", ""),
            ("pk_", ""),
            ("ref_", ""),
            ("reference_", ""),
        ]

        best_match = None
        best_score = 0.0

        # Try removing common patterns and matching
        for pattern, replacement in common_patterns:
            if fragment_lower.endswith(pattern):
                base = fragment_lower[: -len(pattern)]
                if base in self._vertex_lower_map:
                    # High score for pattern-based matches
                    score = 0.9
                    if score > best_score:
                        best_score = score
                        best_match = self._vertex_lower_map[base]
            elif fragment_lower.startswith(pattern):
                base = fragment_lower[len(pattern) :]
                if base in self._vertex_lower_map:
                    score = 0.9
                    if score > best_score:
                        best_score = score
                        best_match = self._vertex_lower_map[base]

        return (best_match, best_score)


class FuzzyMatchCache:
    """Cache for fuzzy matching fragments to vertex names.

    Pre-computes fuzzy matches for all fragments to avoid redundant computations.
    This significantly improves performance when processing multiple tables.
    """

    def __init__(self, vertex_names: list[str], threshold: float = 0.6):
        """Initialize the fuzzy match cache.

        Args:
            vertex_names: List of vertex table names to match against
            threshold: Similarity threshold (0.0 to 1.0)
        """
        self.vertex_names = vertex_names
        self.threshold = threshold
        self._matcher = FuzzyMatcher(vertex_names, threshold)
        self._cache: dict[str, str | None] = {}
        self._build_cache()

    def _build_cache(self) -> None:
        """Pre-compute fuzzy matches for common patterns."""
        # Pre-compute exact matches (case-insensitive)
        for vertex_name in self.vertex_names:
            vertex_lower = vertex_name.lower()
            self._cache[vertex_lower] = vertex_name
            # Also cache common variations
            for suffix in ["id", "fk", "key", "pk", "ref", "reference"]:
                self._cache[f"{vertex_lower}_{suffix}"] = vertex_name
                self._cache[f"{suffix}_{vertex_lower}"] = vertex_name

    def get_match(self, fragment: str) -> str | None:
        """Get cached fuzzy match for a fragment, computing if not cached.

        Args:
            fragment: Fragment to match

        Returns:
            Best matching vertex name or None if no match above threshold
        """
        fragment_lower = fragment.lower()

        # Check cache first
        if fragment_lower in self._cache:
            return self._cache[fragment_lower]

        # Compute match if not cached using improved matcher
        match, _ = self._matcher.match(fragment)
        self._cache[fragment_lower] = match
        return match

    def batch_match(self, fragments: list[str]) -> dict[str, str | None]:
        """Match multiple fragments in batch, using cache when possible.

        Args:
            fragments: List of fragments to match

        Returns:
            Dictionary mapping fragments to their matched vertex names (or None)
        """
        results = {}
        for fragment in fragments:
            results[fragment] = self.get_match(fragment)
        return results
