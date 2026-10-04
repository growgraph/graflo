"""Filter expression system for database queries.

This module provides a flexible system for creating and evaluating filter expressions
that can be translated into different database query languages (AQL, Cypher, Python).
It includes classes for logical operators, comparison operators, and filter clauses.

Key Components:
    - LogicalOperator: Enum for logical operations (AND, OR, NOT, IMPLICATION)
    - ComparisonOperator: Enum for comparison operations (==, !=, >, <, etc.)
    - FilterExpression: Unified filter expression (discriminated: kind="leaf" or kind="composite")

Example:
    >>> expr = FilterExpression.from_dict({
    ...     "AND": [
    ...         {"field": "age", "cmp_operator": ">=", "value": 18},
    ...         {"field": "status", "cmp_operator": "==", "value": "active"}
    ...     ]
    ... })
    >>> # Converts to: "age >= 18 AND status == 'active'"
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Mapping
from datetime import date, datetime, time
from types import MappingProxyType
from typing import Any, Literal, Self, cast

from pydantic import Field, field_validator, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import BaseEnum, ExpressionFlavor

logger = logging.getLogger(__name__)


class LogicalOperator(BaseEnum):
    """Logical operators for combining filter conditions.

    Attributes:
        AND: Logical AND operation
        OR: Logical OR operation
        NOT: Logical NOT operation
        IMPLICATION: Logical IF-THEN operation
    """

    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    IMPLICATION = "IF_THEN"


def implication(ops):
    """Evaluate logical implication (IF-THEN).

    Args:
        ops: Tuple of (antecedent, consequent)

    Returns:
        bool: True if antecedent is False or consequent is True
    """
    a, b = ops
    return b if a else True


OperatorMapping = MappingProxyType(
    {
        LogicalOperator.AND: all,
        LogicalOperator.OR: any,
        LogicalOperator.IMPLICATION: implication,
    }
)

_LOGICAL_OPERATOR_JSON_VALUES: frozenset[str] = frozenset(
    m.value for m in LogicalOperator
)


class ComparisonOperator(BaseEnum):
    """Comparison operators for field comparisons.

    Attributes:
        NEQ: Not equal (!=)
        EQ: Equal (==)
        GE: Greater than or equal (>=)
        LE: Less than or equal (<=)
        GT: Greater than (>)
        LT: Less than (<)
        IN: Membership test (IN)
        IS_NULL: Null check (IS NULL)
        IS_NOT_NULL: Non-null check (IS NOT NULL)
    """

    NEQ = "!="
    EQ = "=="
    GE = ">="
    LE = "<="
    GT = ">"
    LT = "<"
    IN = "IN"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"


DUNDER_TO_CMP: MappingProxyType[str, ComparisonOperator] = MappingProxyType(
    {
        "__eq__": ComparisonOperator.EQ,
        "__ne__": ComparisonOperator.NEQ,
        "__gt__": ComparisonOperator.GT,
        "__lt__": ComparisonOperator.LT,
        "__ge__": ComparisonOperator.GE,
        "__le__": ComparisonOperator.LE,
    }
)

#: Keys a leaf entry may carry. ``operator`` is the authored spelling of
#: ``unary_op``.
_LEAF_KEYS: frozenset[str] = frozenset(
    {"kind", "field", "value", "cmp_operator", "unary_op", "operator"}
)

_FOO_REMOVED = (
    "filter key `foo` was removed: spell the comparison `operator` "
    "(for example `operator: __gt__`)"
)

#: Inverse of :data:`DUNDER_TO_CMP`, so expressions authored in list form
#: (``["==", value, field]``) — which carry no ``unary_op`` — can still be
#: evaluated in Python, like every other flavor renders them.
CMP_TO_DUNDER: MappingProxyType[ComparisonOperator, str] = MappingProxyType(
    {cmp: dunder for dunder, cmp in DUNDER_TO_CMP.items()}
)


#: Characters a string literal writes as an escape, in every graph query language.
_LITERAL_ESCAPES = {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"}
_LITERAL_ESCAPED = re.compile(r'[\\"\n\r\t]')
#: Control characters with no escape common to those languages.
_UNWRITABLE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class BoundParams:
    """Values a rendered filter names by placeholder, for the driver to bind.

    Pass one instance per query as ``params=`` to every filter rendered into
    it, and the query's :attr:`values` to the driver: placeholder names run on
    across filters. Only flavors whose drivers bind parameters take one; nGQL
    and GSQL filters are written as escaped literals.
    """

    _PLACEHOLDERS: Mapping[ExpressionFlavor, str] = MappingProxyType(
        {
            ExpressionFlavor.AQL: "@{}",
            ExpressionFlavor.CYPHER: "${}",
            ExpressionFlavor.SQL: "%({})s",
        }
    )

    def __init__(self, kind: ExpressionFlavor, prefix: str = "f") -> None:
        if kind not in self._PLACEHOLDERS:
            raise ValueError(
                f"{kind} filters take no bound parameters; they are written as "
                "escaped literals"
            )
        self.kind = kind
        self._prefix = prefix
        self.values: dict[str, Any] = {}

    def bind(self, value: Any) -> str:
        """Record *value* and return the placeholder that names it."""
        name = f"{self._prefix}{len(self.values)}"
        self.values[name] = value
        return self._PLACEHOLDERS[self.kind].format(name)


class FilterExpression(ConfigBaseModel):
    """Unified filter expression (discriminated: leaf or composite).

    - kind="leaf": single field comparison (field, cmp_operator, value, optional unary_op).
    - kind="composite": logical combination (operator AND/OR/NOT/IF_THEN, deps).
    """

    kind: Literal["leaf", "composite"]

    # Leaf fields (used when kind="leaf")
    cmp_operator: ComparisonOperator | None = None
    value: list[Any] = Field(default_factory=list)
    field: str | None = None
    unary_op: str | None = (
        None  # optional operator before comparison (YAML key: "operator")
    )

    # Composite fields (used when kind="composite")
    operator: LogicalOperator | None = None  # AND, OR, NOT, IF_THEN
    deps: list[FilterExpression] = Field(default_factory=list)

    @field_validator("value", mode="before")
    @classmethod
    def value_to_list(cls, v: list[Any] | Any) -> list[Any]:
        """Convert single value to list if necessary. Explicit None becomes [None] for null comparison."""
        if v is None:
            return [None]
        if isinstance(v, list):
            return v
        return [v]

    @model_validator(mode="before")
    @classmethod
    def leaf_operator_to_unary_op(cls, data: Any) -> Any:
        """Map leaf 'operator' (YAML/kwargs) to unary_op; infer kind and cmp_operator."""
        if not isinstance(data, dict):
            return data
        data = dict(data)
        if data.get("kind") == "composite":
            return data

        if data.get("kind") is None:
            op = data.get("operator")
            if isinstance(op, LogicalOperator):
                data["kind"] = "composite"
                return data
            if isinstance(op, str) and op in _LOGICAL_OPERATOR_JSON_VALUES:
                data["kind"] = "composite"
                return data
            deps = data.get("deps")
            if isinstance(deps, list) and len(deps) > 0:
                data["kind"] = "composite"
                return data
            if data.get("cmp_operator") is not None or data.get("field") is not None:
                data["kind"] = "leaf"

        if "foo" in data:
            raise ValueError(_FOO_REMOVED)
        raw_op = None
        if "operator" in data and isinstance(data["operator"], str):
            raw_op = data.pop("operator")
        if raw_op is not None:
            data["unary_op"] = raw_op
            if data.get("cmp_operator") is None and raw_op in DUNDER_TO_CMP:
                data["cmp_operator"] = DUNDER_TO_CMP[raw_op]
            if data.get("kind") is None:
                data["kind"] = "leaf"
        return data

    @model_validator(mode="after")
    def check_discriminated_shape(self) -> FilterExpression:
        """Enforce exactly one shape per kind and normalise null-check operators."""
        if self.kind == "leaf":
            if self.operator is not None or self.deps:
                raise ValueError("leaf expression must not have operator or deps")
            if self.cmp_operator is None and self.unary_op is None:
                raise ValueError(
                    "leaf expression requires cmp_operator or operator"
                    + (f" (field {self.field!r})" if self.field else "")
                )
            # IS_NULL / IS_NOT_NULL are unary; clear any spurious value list
            if self.cmp_operator in (
                ComparisonOperator.IS_NULL,
                ComparisonOperator.IS_NOT_NULL,
            ):
                object.__setattr__(self, "value", [])
        else:
            if self.operator is None:
                raise ValueError("composite expression must have operator")
        return self

    @field_validator("deps", mode="before")
    @classmethod
    def parse_deps(cls, v: list[Any]) -> list[Any]:
        """Parse dict/list items into FilterExpression instances."""
        if not isinstance(v, list):
            return v
        result = []
        for item in v:
            if isinstance(item, (dict, list)):
                # Not `from_dict`: a serialized expression (`kind` / `operator`
                # + `deps`) has to come back as the expression it was.
                result.append(parse_filter_expression(item))
            else:
                result.append(item)
        return result

    @classmethod
    def from_list(cls, current: list[Any]) -> FilterExpression:
        """Build a leaf expression from list form [cmp_operator, value, field?, unary_op?]."""
        cmp_operator = current[0]
        value = current[1]
        field = current[2] if len(current) > 2 else None
        unary_op = current[3] if len(current) > 3 else None
        return cls(
            kind="leaf",
            cmp_operator=cmp_operator,
            value=value,
            field=field,
            unary_op=unary_op,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any] | list[Any]) -> Self:
        """Create a filter expression from a dictionary or list.

        Returns FilterExpression (leaf or composite). LSP-compliant: return type is Self.
        """
        if isinstance(data, list):
            if data[0] in ComparisonOperator:
                return cast(Self, cls.from_list(data))
            elif data[0] in LogicalOperator:
                return cls(kind="composite", operator=data[0], deps=data[1])
            raise ValueError(
                f"filter list must start with a comparison or logical operator, "
                f"got {data[0]!r}"
            )
        elif isinstance(data, dict):
            if data.get("kind") is not None:
                return cls.model_validate(data)
            k = next(iter(data.keys()))
            norm_k = k.upper() if isinstance(k, str) else k
            if norm_k in LogicalOperator:
                if len(data) > 1:
                    raise ValueError(
                        "a logical filter entry takes one logical operator key, "
                        f"got {sorted(str(key) for key in data)}"
                    )
                deps: list[FilterExpression] = [cls.from_dict(v) for v in data[k]]
                return cls(
                    kind="composite", operator=LogicalOperator(norm_k), deps=deps
                )
            else:
                # The model is built by hand below, so `extra="forbid"` never
                # sees these keys: an unknown one would be dropped silently.
                if "foo" in data:
                    raise ValueError(_FOO_REMOVED)
                unknown = sorted(str(key) for key in data if key not in _LEAF_KEYS)
                if unknown:
                    raise ValueError(
                        f"unknown filter key(s) {unknown}; a leaf takes "
                        f"{sorted(_LEAF_KEYS - {'kind', 'unary_op'})}, a "
                        f"logical entry one of {sorted(_LOGICAL_OPERATOR_JSON_VALUES)}"
                    )
                unary_op = data.get("operator")
                cmp_operator = data.get("cmp_operator")
                if cmp_operator is None and unary_op is not None:
                    cmp_operator = DUNDER_TO_CMP.get(unary_op)
                return cls(
                    kind="leaf",
                    cmp_operator=cmp_operator,
                    value=data.get("value", []),
                    field=data.get("field"),
                    unary_op=unary_op,
                )
        raise ValueError(f"expected dict or list, got {type(data)}")

    def rename_fields(self, renames: Mapping[str, str]) -> FilterExpression:
        """This expression with every leaf ``field`` rewritten by *renames*.

        Pure: returns a new expression and leaves this one untouched. Fields
        absent from *renames* keep their name.
        """
        if not renames:
            return self
        if self.kind == "leaf":
            if self.field is None or self.field not in renames:
                return self
            return self.model_copy(update={"field": renames[self.field]})
        return self.model_copy(
            update={"deps": [dep.rename_fields(renames) for dep in self.deps]}
        )

    def matches(self, doc: Mapping[str, Any]) -> bool:
        """Whether *doc* satisfies the expression, evaluated in Python.

        Prefer this to ``expr(kind=ExpressionFlavor.PYTHON, **doc)``, which
        cannot evaluate a document with a field named ``kind`` or ``doc_name``.
        """
        if self.kind == "leaf":
            return self._cast_python(doc)
        return self._cast_python_composite(doc)

    def __call__(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.AQL,
        **kwargs,
    ) -> str | bool:
        """Render or evaluate the expression in the target language.

        With ``params=`` a :class:`BoundParams`, values are written as its
        placeholders; without it, as literals.
        """
        if self.kind == "leaf":
            return self._call_leaf(doc_name=doc_name, kind=kind, **kwargs)
        return self._call_composite(doc_name=doc_name, kind=kind, **kwargs)

    def _is_null_operator(self) -> bool:
        """Check if this is a null-checking operator (IS_NULL or IS_NOT_NULL)."""
        return self.cmp_operator in (
            ComparisonOperator.IS_NULL,
            ComparisonOperator.IS_NOT_NULL,
        )

    def _call_leaf(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.AQL,
        **kwargs,
    ) -> str | bool:
        if not self._is_null_operator() and not self.value:
            logger.warning(f"for {self} value is not set : {self.value}")
        if self.cmp_operator is None and kind != ExpressionFlavor.PYTHON:
            raise ValueError(
                "leaf expression requires cmp_operator for non-PYTHON flavor"
            )
        params = _params_for(kind, kwargs)
        if kind == ExpressionFlavor.AQL:
            return self._cast_arango(doc_name, params)
        elif kind == ExpressionFlavor.CYPHER:
            return self._cast_cypher(doc_name, params)
        elif kind == ExpressionFlavor.NGQL:
            return self._cast_ngql(doc_name)
        elif kind == ExpressionFlavor.GSQL:
            if doc_name == "":
                field_types = kwargs.get("field_types")
                return self._cast_restpp(field_types=field_types)
            return self._cast_tigergraph(doc_name)
        elif kind == ExpressionFlavor.SQL:
            return self._cast_sql(params)
        elif kind == ExpressionFlavor.PYTHON:
            return self._cast_python(kwargs)
        raise ValueError(f"kind {kind} not implemented")

    def _call_composite(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.AQL,
        **kwargs,
    ) -> str | bool:
        if kind in (
            ExpressionFlavor.AQL,
            ExpressionFlavor.CYPHER,
            ExpressionFlavor.NGQL,
            ExpressionFlavor.GSQL,
            ExpressionFlavor.SQL,
        ):
            return self._cast_generic(
                doc_name=doc_name, kind=kind, params=_params_for(kind, kwargs)
            )
        elif kind == ExpressionFlavor.PYTHON:
            return self._cast_python_composite(kwargs)
        raise ValueError(f"kind {kind} not implemented")

    def _in_members(self) -> list[Any]:
        """The members an ``IN`` tests against.

        ``value: [[a, b]]`` names the same set as ``value: [a, b]``.
        """
        if len(self.value) == 1 and isinstance(self.value[0], list):
            return list(self.value[0])
        return list(self.value)

    @staticmethod
    def _graph_literal(value: Any) -> str:
        """One value as a literal of the graph query languages (AQL, Cypher, nGQL, GSQL).

        Raises:
            ValueError: For a value with no literal all of them read the same
                way: a control character other than newline, carriage return
                and tab, a non-finite number, or an object of another type.
        """
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"{value!r} has no literal in a query")
            return repr(value)
        if isinstance(value, (datetime, date, time)):
            value = value.isoformat()
        if isinstance(value, str):
            if _UNWRITABLE.search(value):
                raise ValueError(
                    f"{value!r} holds a control character with no literal in a query"
                )
            escaped = _LITERAL_ESCAPED.sub(lambda m: _LITERAL_ESCAPES[m.group()], value)
            return f'"{escaped}"'
        if isinstance(value, (list, tuple)):
            return (
                "[" + ", ".join(FilterExpression._graph_literal(v) for v in value) + "]"
            )
        raise ValueError(
            f"a {type(value).__name__} value has no literal in a query: {value!r}"
        )

    @staticmethod
    def _sql_literal(value: Any) -> str:
        """One value as a SQL literal: strings and ISO datetimes single-quoted."""
        if value is None:
            return "null"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _cast_value(self, params: BoundParams | None = None) -> str:
        """The compared value: a placeholder of *params*, or a literal without it."""
        if self.cmp_operator == ComparisonOperator.IN:
            # Always a list, whatever its length: `x IN "a"` is not membership.
            if params is not None:
                return params.bind(self._in_members())
            members = ", ".join(self._graph_literal(v) for v in self._in_members())
            return f"[{members}]"
        operand: Any = self.value[0] if len(self.value) == 1 else list(self.value)
        if params is not None:
            return params.bind(operand)
        return self._graph_literal(operand)

    def _query_unary_op(self) -> str | None:
        """``unary_op`` as query text, or ``None`` when it only spells ``cmp_operator``.

        A dunder (``__eq__``) is the Python spelling of the comparison; the
        query languages write the comparison itself.
        """
        if self.unary_op is None or self.unary_op in DUNDER_TO_CMP:
            return None
        return self.unary_op

    def _cast_arango(self, doc_name: str, params: BoundParams | None = None) -> str:
        if self.cmp_operator == ComparisonOperator.IS_NULL:
            return f'{doc_name}["{self.field}"] == null'
        if self.cmp_operator == ComparisonOperator.IS_NOT_NULL:
            return f'{doc_name}["{self.field}"] != null'
        const = self._cast_value(params)
        lemma = f"{self.cmp_operator} {const}"
        if self._query_unary_op() is not None:
            lemma = f"{self._query_unary_op()} {lemma}"
        if self.field is not None:
            lemma = f'{doc_name}["{self.field}"] {lemma}'
        return lemma

    def _cast_cypher(self, doc_name: str, params: BoundParams | None = None) -> str:
        if self.cmp_operator == ComparisonOperator.IS_NULL:
            return f"{doc_name}.{self.field} IS NULL"
        if self.cmp_operator == ComparisonOperator.IS_NOT_NULL:
            return f"{doc_name}.{self.field} IS NOT NULL"
        const = self._cast_value(params)
        cmp_op = (
            "=" if self.cmp_operator == ComparisonOperator.EQ else self.cmp_operator
        )
        lemma = f"{cmp_op} {const}"
        if self._query_unary_op() is not None:
            lemma = f"{self._query_unary_op()} {lemma}"
        if self.field is not None:
            lemma = f"{doc_name}.{self.field} {lemma}"
        return lemma

    def _cast_ngql(self, doc_name: str) -> str:
        """Render leaf as nGQL expression (NebulaGraph 3.x).

        Uses dot-access like Cypher but keeps ``==`` for equality (nGQL standard).
        The caller passes *doc_name* as ``"v.TagName"`` so property access becomes
        ``v.TagName.field``.
        """
        if self.cmp_operator == ComparisonOperator.IS_NULL:
            return f"{doc_name}.{self.field} IS EMPTY"
        if self.cmp_operator == ComparisonOperator.IS_NOT_NULL:
            return f"{doc_name}.{self.field} IS NOT EMPTY"
        const = self._cast_value()
        lemma = f"{self.cmp_operator} {const}"
        if self._query_unary_op() is not None:
            lemma = f"{self._query_unary_op()} {lemma}"
        if self.field is not None:
            lemma = f"{doc_name}.{self.field} {lemma}"
        return lemma

    def _cast_tigergraph(self, doc_name: str) -> str:
        if self.cmp_operator == ComparisonOperator.IS_NULL:
            return f"{doc_name}.{self.field} IS NULL"
        if self.cmp_operator == ComparisonOperator.IS_NOT_NULL:
            return f"{doc_name}.{self.field} IS NOT NULL"
        const = self._cast_value()
        cmp_op = (
            "==" if self.cmp_operator == ComparisonOperator.EQ else self.cmp_operator
        )
        lemma = f"{cmp_op} {const}"
        if self._query_unary_op() is not None:
            lemma = f"{self._query_unary_op()} {lemma}"
        if self.field is not None:
            lemma = f"{doc_name}.{self.field} {lemma}"
        return lemma

    @staticmethod
    def _quote_sql_field(field: str) -> str:
        """Quote a SQL field name, handling dotted alias.column references.

        ``record_id``   -> ``"record_id"``
        ``s.record_id`` -> ``s."record_id"``
        """
        if "." in field:
            alias, col = field.split(".", 1)
            return f'{alias}."{col}"'
        return f'"{field}"'

    def _cast_sql(self, params: BoundParams | None = None) -> str:
        """Render leaf as SQL WHERE fragment: \"column\" op value.

        Values are placeholders of *params*, or without it literals with
        strings and dates single-quoted.
        """
        if not self.field:
            # An empty fragment would be dropped from the WHERE clause, and the
            # query would run without the condition.
            raise ValueError(
                f"a SQL condition needs a field: {self.cmp_operator} {self.value}"
            )
        quoted = self._quote_sql_field(self.field)
        if self.cmp_operator == ComparisonOperator.IS_NULL:
            return f"{quoted} IS NULL"
        if self.cmp_operator == ComparisonOperator.IS_NOT_NULL:
            return f"{quoted} IS NOT NULL"
        if self.cmp_operator == ComparisonOperator.IN:
            members = self._in_members()
            if not members:
                raise ValueError(f"IN on {self.field!r} needs at least one value")
            if params is not None:
                rendered = ", ".join(params.bind(v) for v in members)
            else:
                rendered = ", ".join(self._sql_literal(v) for v in members)
            return f"{quoted} IN ({rendered})"
        if self.cmp_operator == ComparisonOperator.EQ:
            op_str = "="
        elif self.cmp_operator == ComparisonOperator.NEQ:
            op_str = "!="
        elif self.cmp_operator in (
            ComparisonOperator.GT,
            ComparisonOperator.LT,
            ComparisonOperator.GE,
            ComparisonOperator.LE,
        ):
            op_str = str(self.cmp_operator)
        else:
            op_str = str(self.cmp_operator)
        value = self.value[0] if self.value else None
        if params is not None:
            return f"{quoted} {op_str} {params.bind(value)}"
        return f"{quoted} {op_str} {self._sql_literal(value)}"

    def _cast_restpp(self, field_types: dict[str, Any] | None = None) -> str:
        if not self.field:
            return ""
        if self.cmp_operator == ComparisonOperator.IS_NULL:
            return f'{self.field}=""'
        if self.cmp_operator == ComparisonOperator.IS_NOT_NULL:
            return f'{self.field}!=""'
        if self.cmp_operator == ComparisonOperator.IN:
            # The REST filter syntax compares one attribute with one value.
            raise ValueError(
                f"IN on {self.field!r} is not expressible as a TigerGraph REST filter"
            )
        if self.cmp_operator == ComparisonOperator.EQ:
            op_str = "="
        elif self.cmp_operator == ComparisonOperator.NEQ:
            op_str = "!="
        elif self.cmp_operator == ComparisonOperator.GT:
            op_str = ">"
        elif self.cmp_operator == ComparisonOperator.LT:
            op_str = "<"
        elif self.cmp_operator == ComparisonOperator.GE:
            op_str = ">="
        elif self.cmp_operator == ComparisonOperator.LE:
            op_str = "<="
        else:
            op_str = str(self.cmp_operator)
        value = self.value[0] if self.value else None
        if value is None:
            value_str = "null"
        elif isinstance(value, (int, float)):
            value_str = str(value)
        elif isinstance(value, str):
            if '"' in value or "," in value:
                # A comma separates REST filter conditions and a quote ends the
                # value; there is no escape for either.
                raise ValueError(
                    f"{value!r} cannot be written in a TigerGraph REST filter"
                )
            is_string_field = True
            if field_types and self.field in field_types:
                field_type = field_types[self.field]
                field_type_str = (
                    field_type.value
                    if hasattr(field_type, "value")
                    else str(field_type).upper()
                )
                if field_type_str in ("INT", "UINT", "FLOAT", "DOUBLE"):
                    is_string_field = False
            value_str = f'"{value}"' if is_string_field else str(value)
        else:
            value_str = str(value)
        return f"{self.field}{op_str}{value_str}"

    def _cast_python(self, doc: Mapping[str, Any]) -> bool:
        if self.field is None:
            return False
        field_val = doc.get(self.field)
        if self.cmp_operator == ComparisonOperator.IS_NULL:
            return field_val is None
        if self.cmp_operator == ComparisonOperator.IS_NOT_NULL:
            return field_val is not None
        if field_val is None:
            return False
        if self.cmp_operator == ComparisonOperator.IN:
            return field_val in self._in_members()
        # List-form expressions (["==", value, field]) carry no unary_op; fall
        # back to the dunder implied by cmp_operator so they evaluate the same
        # way they render.
        dunder = self.unary_op
        if dunder is None and self.cmp_operator is not None:
            dunder = CMP_TO_DUNDER.get(self.cmp_operator)
        if dunder is None or not self.value:
            return False
        comparison = getattr(field_val, dunder, None)
        if comparison is None:
            return False
        result = comparison(self.value[0])
        return result is True

    @staticmethod
    def _wrap_composite_operand(
        dep: FilterExpression, rendered: str, kind: ExpressionFlavor
    ) -> str:
        """Parenthesize nested composite operands so precedence is explicit.

        Every target language binds AND tighter than OR, so an unparenthesized
        ``AND[OR[a, b], c]`` silently means ``a OR (b AND c)``. Wrapping nested
        composites keeps the rendered predicate faithful to the expression tree
        on all flavors.
        """
        if (
            kind
            in (
                ExpressionFlavor.SQL,
                ExpressionFlavor.CYPHER,
                ExpressionFlavor.AQL,
                ExpressionFlavor.NGQL,
                ExpressionFlavor.GSQL,
            )
            and dep.kind == "composite"
        ):
            return f"({rendered})"
        return rendered

    def _render_dep(
        self,
        dep: FilterExpression,
        doc_name: str,
        kind: ExpressionFlavor,
        params: BoundParams | None = None,
    ) -> str:
        rendered = str(dep(kind=kind, doc_name=doc_name, params=params))
        return self._wrap_composite_operand(dep, rendered, kind)

    def _cast_generic(
        self, doc_name: str, kind: ExpressionFlavor, params: BoundParams | None = None
    ) -> str:
        if self.operator is None:
            raise ValueError("composite expression requires operator")
        if (
            kind == ExpressionFlavor.SQL
            and self.operator == LogicalOperator.IMPLICATION
        ):
            if len(self.deps) != 2:
                raise ValueError("IF_THEN composite requires exactly 2 deps")
            antecedent = self._render_dep(self.deps[0], doc_name, kind, params)
            consequent = self._render_dep(self.deps[1], doc_name, kind, params)
            return f"(NOT ({antecedent}) OR ({consequent}))"
        if len(self.deps) == 1:
            if self.operator == LogicalOperator.NOT:
                result = self._render_dep(self.deps[0], doc_name, kind, params)
                if doc_name == "" and kind == ExpressionFlavor.GSQL:
                    return f"!{result}"
                return f"NOT {result}"
            raise ValueError(
                f" length of deps = {len(self.deps)} but operator is not {LogicalOperator.NOT}"
            )
        deps_str_cast = [
            self._render_dep(dep, doc_name, kind, params) for dep in self.deps
        ]
        if doc_name == "" and kind == ExpressionFlavor.GSQL:
            if self.operator == LogicalOperator.AND:
                return " && ".join(deps_str_cast)
            if self.operator == LogicalOperator.OR:
                return " || ".join(deps_str_cast)
        return f" {self.operator} ".join(deps_str_cast)

    def _cast_python_composite(self, doc: Mapping[str, Any]) -> bool:
        if self.operator is None:
            raise ValueError("composite expression requires operator")
        if len(self.deps) == 1:
            if self.operator == LogicalOperator.NOT:
                return not self.deps[0].matches(doc)
            raise ValueError(
                f" length of deps = {len(self.deps)} but operator is not {LogicalOperator.NOT}"
            )
        return OperatorMapping[self.operator]([dep.matches(doc) for dep in self.deps])


def _params_for(
    kind: ExpressionFlavor, kwargs: Mapping[str, Any]
) -> BoundParams | None:
    """The ``params=`` of a render call, checked against the flavor rendered."""
    params = kwargs.get("params")
    if params is None:
        return None
    if not isinstance(params, BoundParams) or params.kind != kind:
        raise ValueError(f"params= must be a BoundParams for {kind}")
    return params


def render_conjunct(
    expr: FilterExpression,
    *,
    kind: ExpressionFlavor,
    doc_name: str = "doc",
    **kwargs: Any,
) -> str:
    """Render *expr* as one operand of an ``AND`` of conditions.

    Callers that collect several conditions and join them with ``AND`` must
    render each through this function. Every target language binds ``AND``
    tighter than ``OR``, so a bare ``a OR b`` beside ``c`` would mean
    ``a OR (b AND c)``; an ``OR`` condition is therefore parenthesised. ``AND``
    and ``NOT`` conditions and single comparisons keep their meaning unwrapped,
    and ``IF_THEN`` renders its own parentheses.
    """
    rendered = str(expr(doc_name=doc_name, kind=kind, **kwargs))
    if expr.kind == "composite" and expr.operator == LogicalOperator.OR:
        return f"({rendered})"
    return rendered


def parse_filter_expression(raw: Any) -> FilterExpression:
    """Parse YAML/JSON/dict/list/filter model into a :class:`FilterExpression`.

    Uses :meth:`FilterExpression.from_dict` for logical shorthand (``OR``, ``AND``,
    ``NOT``, ``IF_THEN`` keys) and leaf shorthand; uses ``model_validate`` for
    discriminated ``kind`` / ``operator`` + ``deps`` payloads.
    """
    if isinstance(raw, FilterExpression):
        return raw
    if isinstance(raw, dict):
        if raw.get("kind") is not None:
            return FilterExpression.model_validate(raw)
        keys = list(raw.keys())
        if len(keys) == 1:
            norm_k = keys[0].upper() if isinstance(keys[0], str) else keys[0]
            if norm_k in _LOGICAL_OPERATOR_JSON_VALUES:
                return FilterExpression.from_dict(raw)
        if "operator" in raw or "deps" in raw:
            return FilterExpression.model_validate(raw)
        return FilterExpression.from_dict(raw)
    if isinstance(raw, list):
        return FilterExpression.from_dict(raw)
    raise ValueError(
        f"expected FilterExpression, dict, or list, got {type(raw).__name__}"
    )
