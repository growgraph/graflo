"""Filter expression system for database queries.

This package provides a flexible system for creating and evaluating filter expressions
that can be translated into different database query languages (AQL, Cypher, SQL, Python).

Key Components:
    - LogicalOperator: Logical operations (AND, OR, NOT, IMPLICATION / IF_THEN)
    - ComparisonOperator: Comparison operations (==, !=, >, <, etc.)
    - FilterExpression: Filter expression (leaf or composite logical formulae)
    - parse_filter_expression: Unified YAML/JSON loader (Bindings, SelectSpec, graph DB)

SQL notes:
    - IF_THEN (IMPLICATION) renders as ``(NOT antecedent OR consequent)``.
    - Use ExpressionFlavor.PYTHON for in-memory implication evaluation on documents.

Example:
    >>> from graflo.filter import FilterExpression, parse_filter_expression
    >>> expr = parse_filter_expression({
    ...     "AND": [
    ...         {"field": "age", "cmp_operator": ">=", "value": 18},
    ...         {"field": "status", "cmp_operator": "==", "value": "active"},
    ...     ]
    ... })
"""

from .onto import (
    ComparisonOperator,
    FilterExpression,
    LogicalOperator,
    parse_filter_expression,
)
from .select import ALL_BASE_COLUMNS, SelectSpec

__all__ = [
    "ALL_BASE_COLUMNS",
    "ComparisonOperator",
    "FilterExpression",
    "LogicalOperator",
    "SelectSpec",
    "parse_filter_expression",
]
