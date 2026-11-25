"""Miscellaneous utility functions.

This module provides various utility functions for data manipulation and processing.

Key Functions:
    - sorted_dicts: Sort lists of flat dictionaries for consistent ordering
"""

from typing import Any


def _is_flat_dict(value: Any) -> bool:
    return isinstance(value, dict) and not any(
        isinstance(v, (list, tuple, dict)) for v in value.values()
    )


def sorted_dicts(d: Any) -> Any:
    """Sort lists of flat dictionaries for consistent ordering.

    A non-empty list or tuple whose elements are all dictionaries of scalar
    values is returned as a list sorted by the items of each dictionary. A
    dictionary is rebuilt with its nested lists, tuples and dictionaries
    processed the same way; its keys keep their order. Any other value,
    including any other list, is returned unchanged.

    Args:
        d: Data structure to sort

    Returns:
        The data structure with its lists of flat dictionaries sorted

    Example:
        >>> sorted_dicts({"b": 2, "c": [{"y": 2}, {"x": 1}], "d": [3, 1]})
        {'b': 2, 'c': [{'x': 1}, {'y': 2}], 'd': [3, 1]}
    """
    if isinstance(d, (tuple, list)):
        if d and all(_is_flat_dict(dd) for dd in d):
            return sorted(d, key=lambda x: tuple(x.items()))
    elif isinstance(d, dict):
        return {
            k: v if not isinstance(v, (list, tuple, dict)) else sorted_dicts(v)
            for k, v in d.items()
        }

    return d
