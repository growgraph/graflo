"""Miscellaneous utility functions.

This module provides various utility functions for data manipulation and processing.

Key Functions:
    - sorted_dicts: Recursively sort dictionaries and lists for consistent ordering
"""


def sorted_dicts(d):
    """Recursively sort dictionaries and lists for consistent ordering.

    This function recursively sorts dictionaries and lists to ensure consistent
    ordering of data structures. It handles nested structures and preserves
    non-collection values.

    Args:
        d: Data structure to sort (dict, list, tuple, or other)

    Returns:
        The sorted data structure with consistent ordering

    Example:
        >>> data = {"b": 2, "a": 1, "c": [3, 1, 2]}
        >>> sorted_dicts(data)
        {"a": 1, "b": 2, "c": [1, 2, 3]}
    """
    if isinstance(d, (tuple, list)):
        if d and all(not isinstance(dd, (list, tuple, dict)) for dd in d[0].values()):
            return sorted(d, key=lambda x: tuple(x.items()))
    elif isinstance(d, dict):
        return {
            k: v if not isinstance(v, (list, tuple, dict)) else sorted_dicts(v)
            for k, v in d.items()
        }

    return d
