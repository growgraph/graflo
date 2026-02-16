"""SQL filter helpers built on top of FilterExpression.

Provides utility functions for generating SQL WHERE fragments from
structured filter parameters.
"""

from __future__ import annotations

from typing import cast

from graflo.filter.onto import (
    ComparisonOperator,
    FilterExpression,
    LogicalOperator,
)
from graflo.onto import ExpressionFlavor


def datetime_range_where_sql(
    datetime_after: str | None,
    datetime_before: str | None,
    date_column: str,
) -> str:
    """Build SQL WHERE fragment for [datetime_after, datetime_before) via FilterExpression.

    Returns empty string if both bounds are None; otherwise uses column with >= and <.
    """
    if not datetime_after and not datetime_before:
        return ""
    parts: list[FilterExpression] = []
    if datetime_after is not None:
        parts.append(
            FilterExpression(
                kind="leaf",
                field=date_column,
                cmp_operator=ComparisonOperator.GE,
                value=[datetime_after],
            )
        )
    if datetime_before is not None:
        parts.append(
            FilterExpression(
                kind="leaf",
                field=date_column,
                cmp_operator=ComparisonOperator.LT,
                value=[datetime_before],
            )
        )
    if len(parts) == 1:
        return cast(str, parts[0](kind=ExpressionFlavor.SQL))
    expr = FilterExpression(
        kind="composite",
        operator=LogicalOperator.AND,
        deps=parts,
    )
    return cast(str, expr(kind=ExpressionFlavor.SQL))
