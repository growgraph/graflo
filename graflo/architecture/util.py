"""Utility functions for graph architecture operations.

This module provides utility functions for working with graph data structures
and transformations. It includes functions for dictionary projection and
graph entity name formatting.

Key Functions:
    - project_dict: Project dictionary fields based on inclusion/exclusion
    - cast_graph_name_to_triple: Convert graph names to standardized triple format

Example:
    >>> data = {"a": 1, "b": 2, "c": 3}
    >>> project_dict(data, ["a", "b"], how="include")
    {'a': 1, 'b': 2}
    >>> cast_graph_name_to_triple("user_post_graph")
    ('user', 'post', None)
"""

from graflo.architecture.onto import GraphEntity


def project_dict(item, keys, how="include"):
    """Project dictionary fields based on inclusion or exclusion.

    This function filters a dictionary based on a list of keys, either including
    or excluding the specified keys.

    Args:
        item: Dictionary to project
        keys: List of keys to include or exclude
        how: Projection mode - "include" or "exclude" (default: "include")

    Returns:
        dict: Projected dictionary containing only the specified fields

    Example:
        >>> data = {"a": 1, "b": 2, "c": 3}
        >>> project_dict(data, ["a", "b"], how="include")
        {'a': 1, 'b': 2}
        >>> project_dict(data, ["a"], how="exclude")
        {'b': 2, 'c': 3}
    """
    if how == "include":
        return {k: v for k, v in item.items() if k in keys}
    elif how == "exclude":
        return {k: v for k, v in item.items() if k not in keys}
    else:
        return {}


def cast_graph_name_to_triple(s: GraphEntity) -> str | tuple:
    """Convert a graph name string to a triple format.

    This function parses graph entity names into a standardized triple format
    (source, target, type). It handles various naming patterns and special
    suffixes like "graph" or "edges".

    Args:
        s: Graph entity name or ID

    Returns:
        str | tuple: Either a string for simple names or a tuple
            representing (source, target, type) for complex names

    Raises:
        ValueError: If the graph name cannot be cast to a valid format

    Example:
        >>> cast_graph_name_to_triple("user_post_graph")
        ('user', 'post', None)
        >>> cast_graph_name_to_triple("simple_vertex")
        ('simple', None)
    """
    if isinstance(s, str):
        s2 = s.split("_")
        if len(s2) < 2:
            return s2[0]
        elif len(s2) == 2:
            return *s2[:-1], None
        elif len(s2) == 3:
            if s2[-1] in ["graph", "edges"]:
                return *s2[:-1], None
            else:
                return tuple(s2)
        elif len(s2) == 4 and s2[-1] in ["graph", "edges"]:
            return tuple(s2[:-1])
        raise ValueError(f"Invalid graph_name {s} : can not be cast to GraphEntity")
    else:
        return s
