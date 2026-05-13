"""Shared datetime / timestamp column filter for resource connectors.

Uses pandas :class:`pandas.Timedelta` strings for ``interval`` (e.g. ``\"7D\"``,
``\"2H\"``). Default range semantics match half-open intervals ``[start, end)``
(``>=`` lower, ``<`` upper) when ``start_inclusive`` / ``end_inclusive`` are left
at their defaults.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from typing import TYPE_CHECKING, Self

import pandas as pd
from pydantic import Field, model_validator

from graflo.architecture.base import ConfigBaseModel

if TYPE_CHECKING:
    from graflo.filter.onto import FilterExpression

_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}\Z")


def parse_iso_date_or_datetime(raw: str) -> tuple[datetime, bool]:
    """Parse *raw* to a naive ``datetime`` and whether the input was date-only.

    Date-only inputs (``YYYY-MM-DD``) are interpreted at midnight for arithmetic;
    ``is_date_only`` controls SQL literal formatting for day-granular bounds.
    """
    value = raw.strip().strip("'")
    if _DATE_ONLY.match(value):
        d = date.fromisoformat(value)
        return datetime.combine(d, time.min), True
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"Invalid ISO date or datetime: {raw!r}") from e
    return dt, False


def format_sql_literal(dt: datetime, is_date_only: bool) -> str:
    """Format *dt* as a string suitable for ``FilterExpression`` SQL rendering."""
    if is_date_only and dt.time() == time.min:
        return dt.date().isoformat()
    return dt.isoformat(sep=" ", timespec="seconds")


class ColumnTimeFilter(ConfigBaseModel):
    """Predicate on a single date/time column (SQL-friendly via :class:`FilterExpression`)."""

    column: str = Field(..., description="Column name for the time predicate.")
    start: str | None = Field(
        default=None,
        description="Lower bound (ISO date or datetime). Interpreted with start_inclusive.",
    )
    end: str | None = Field(
        default=None,
        description="Upper bound (ISO date or datetime). Interpreted with end_inclusive.",
    )
    interval: str | None = Field(
        default=None,
        description='Pandas timedelta string (e.g. "7D", "2H"); requires start; '
        "defines [start, start + interval). Mutually exclusive with end.",
    )
    not_equals: str | None = Field(
        default=None,
        description="If set, render column != value. Mutually exclusive with start/end/interval.",
    )
    start_inclusive: bool = Field(
        default=True,
        description="If True (default), lower bound uses >= when start is set.",
    )
    end_inclusive: bool = Field(
        default=False,
        description="If False (default), upper bound uses < when end is set.",
    )

    @model_validator(mode="after")
    def _check_shape(self) -> Self:
        has_pred = any(
            v is not None
            for v in (self.start, self.end, self.interval, self.not_equals)
        )
        if not has_pred:
            # Column-only hint (e.g. introspection sets datetime column without a WHERE).
            return self
        if self.not_equals is not None:
            if any(v is not None for v in (self.start, self.end, self.interval)):
                raise ValueError(
                    "not_equals cannot be combined with start, end, or interval"
                )
            return self
        if self.interval is not None:
            if self.start is None:
                raise ValueError("interval requires start")
            if self.end is not None:
                raise ValueError(
                    "interval cannot be combined with end; use start + interval"
                )
            self._validated_timedelta()
            return self
        if self.start is None and self.end is None:
            raise ValueError(
                "ColumnTimeFilter requires at least one of: start, end, interval, not_equals"
            )
        return self

    def _validated_timedelta(self) -> pd.Timedelta:
        assert self.interval is not None
        try:
            td = pd.Timedelta(self.interval)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid pandas timedelta string for interval: {self.interval!r}"
            ) from e
        if pd.isna(td):
            raise ValueError(
                f"Invalid pandas timedelta string for interval: {self.interval!r}"
            )
        return td

    def _interval_upper_literal(self) -> str:
        assert self.start is not None and self.interval is not None
        start_dt, date_only = parse_iso_date_or_datetime(self.start)
        delta = self._validated_timedelta().to_pytimedelta()
        end_dt = start_dt + delta
        upper_date_only = date_only and (end_dt.time() == time.min)
        return format_sql_literal(end_dt, upper_date_only)

    def _lower_literal(self) -> str:
        assert self.start is not None
        dt, date_only = parse_iso_date_or_datetime(self.start)
        return format_sql_literal(dt, date_only)

    def _upper_literal(self) -> str:
        assert self.end is not None
        dt, date_only = parse_iso_date_or_datetime(self.end)
        return format_sql_literal(dt, date_only)

    def as_filter_expression(self) -> FilterExpression | None:
        """Return a single composite AND of leaves, or None if misconfigured."""
        from graflo.filter.onto import (
            ComparisonOperator,
            FilterExpression,
            LogicalOperator,
        )

        if self.not_equals is not None:
            return FilterExpression(
                kind="leaf",
                field=self.column,
                cmp_operator=ComparisonOperator.NEQ,
                value=[self.not_equals],
            )

        leaves: list[FilterExpression] = []

        if self.interval is not None:
            assert self.start is not None
            # Half-open window [start, start + interval).
            leaves.append(
                FilterExpression(
                    kind="leaf",
                    field=self.column,
                    cmp_operator=ComparisonOperator.GE,
                    value=[self._lower_literal()],
                )
            )
            leaves.append(
                FilterExpression(
                    kind="leaf",
                    field=self.column,
                    cmp_operator=ComparisonOperator.LT,
                    value=[self._interval_upper_literal()],
                )
            )
        else:
            if self.start is not None:
                lower_op = (
                    ComparisonOperator.GE
                    if self.start_inclusive
                    else ComparisonOperator.GT
                )
                leaves.append(
                    FilterExpression(
                        kind="leaf",
                        field=self.column,
                        cmp_operator=lower_op,
                        value=[self._lower_literal()],
                    )
                )
            if self.end is not None:
                upper_op = (
                    ComparisonOperator.LE
                    if self.end_inclusive
                    else ComparisonOperator.LT
                )
                leaves.append(
                    FilterExpression(
                        kind="leaf",
                        field=self.column,
                        cmp_operator=upper_op,
                        value=[self._upper_literal()],
                    )
                )

        if not leaves:
            return None
        if len(leaves) == 1:
            return leaves[0]
        return FilterExpression(
            kind="composite",
            operator=LogicalOperator.AND,
            deps=leaves,
        )
