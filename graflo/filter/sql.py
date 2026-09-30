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


def datetime_range_filter(
    datetime_after: str | None,
    datetime_before: str | None,
    date_column: str,
) -> FilterExpression | None:
    """The half-open range [datetime_after, datetime_before) on *date_column*.

    Returns ``None`` when neither bound is given. The result is an ordinary
    filter, so a connector qualifies and joins it like its declared ones
    (``TableConnector.build_query(extra_filters=...)``).
    """
    if not datetime_after and not datetime_before:
        return None
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
        return parts[0]
    return FilterExpression(
        kind="composite",
        operator=LogicalOperator.AND,
        deps=parts,
    )


def datetime_range_where_sql(
    datetime_after: str | None,
    datetime_before: str | None,
    date_column: str,
) -> str:
    """Build SQL WHERE fragment for [datetime_after, datetime_before) via FilterExpression.

    Returns empty string if both bounds are None; otherwise uses column with >= and <.
    """
    expr = datetime_range_filter(datetime_after, datetime_before, date_column)
    if expr is None:
        return ""
    return cast(str, expr(kind=ExpressionFlavor.SQL))
