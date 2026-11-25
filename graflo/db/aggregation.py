"""The result shape every backend's ``Connection.aggregate`` returns.

Ungrouped, an aggregation returns one value:

- ``COUNT``: an ``int``, the number of documents matched;
- ``MAX`` / ``MIN``: the extreme value, or ``None`` when no document has the field;
- ``AVERAGE``: a ``float``, or ``None`` when no document has the field;
- ``SORTED_UNIQUE``: the distinct values as a list, sorted (see :func:`sort_values`).

Grouped by a field, every function returns ``{group value: what the ungrouped call
returns for the documents of that group}``; documents without the group field form
the ``None`` group, and a group whose documents all lack *aggregated_field* still
appears, with the value of an empty aggregation. A document without
*aggregated_field* contributes nothing to ``MAX``, ``MIN``, ``AVERAGE`` or
``SORTED_UNIQUE``.
"""

from collections.abc import Iterable, Sequence
from typing import Any

from graflo.onto import AggregationType

#: Cypher aggregate per function; ``{}`` is the aggregated expression.
_CYPHER: dict[AggregationType, str] = {
    AggregationType.COUNT: "count(*)",
    AggregationType.MAX: "max({})",
    AggregationType.MIN: "min({})",
    AggregationType.AVERAGE: "avg({})",
    AggregationType.SORTED_UNIQUE: "collect(DISTINCT {})",
}

#: Column names of the rows a backend reads back.
KEY = "group_key"
VALUE = "agg_value"


def check_aggregation(
    function: AggregationType, aggregated_field: str | None
) -> AggregationType:
    """*function* as an ``AggregationType``, once its field requirement is met.

    Raises:
        ValueError: *function* is no ``AggregationType``, or it is not ``COUNT``
            and *aggregated_field* is missing.
    """
    function = AggregationType(function)
    if function != AggregationType.COUNT and not aggregated_field:
        raise ValueError(f"aggregated_field is required for {function}")
    return function


def cypher_aggregation_return(
    function: AggregationType, value: str | None, key: str | None
) -> str:
    """The ``RETURN`` clause of a Cypher aggregation, columns :data:`KEY` and :data:`VALUE`.

    Args:
        function: The aggregation, already checked by :func:`check_aggregation`.
        value: The rendered aggregated expression (``n.price``); unused by ``COUNT``.
        key: The rendered group expression, or ``None`` for one ungrouped row.
    """
    aggregate = _CYPHER[function].format(value)
    if key is None:
        return f"RETURN {aggregate} AS {VALUE}"
    return f"RETURN {key} AS {KEY}, {aggregate} AS {VALUE}"


def _order(value: Any) -> tuple[int, Any]:
    if isinstance(value, (int, float)):
        return (0, value)
    if isinstance(value, str):
        return (1, value)
    return (2, repr(value))


def sort_values(values: Iterable[Any]) -> list[Any]:
    """*values* without ``None``, numbers first, then strings, then the rest by ``repr``.

    A total order, so a field holding mixed types sorts the same on every backend.
    """
    return sorted((v for v in values if v is not None), key=_order)


def hashable_key(key: Any) -> Any:
    """*key* usable as a dict key: lists become tuples, recursively."""
    if isinstance(key, list):
        return tuple(hashable_key(k) for k in key)
    return key


def _finish(function: AggregationType, value: Any) -> Any:
    if function == AggregationType.COUNT:
        return int(value or 0)
    if function == AggregationType.SORTED_UNIQUE:
        return sort_values(value or [])
    if function == AggregationType.AVERAGE and value is not None:
        return float(value)
    return value


def aggregation_result(
    function: AggregationType,
    rows: Sequence[tuple[Any, Any]],
    *,
    grouped: bool,
) -> Any:
    """Shape ``(group value, aggregate)`` rows into the documented result.

    Args:
        function: The aggregation the rows answer.
        rows: One pair per group; ungrouped, at most one pair, whose group value
            is ignored. ``SORTED_UNIQUE`` aggregates are the distinct values in
            any order.
        grouped: Whether the query grouped by a field.
    """
    if grouped:
        return {hashable_key(key): _finish(function, value) for key, value in rows}
    return _finish(function, rows[0][1] if rows else None)
