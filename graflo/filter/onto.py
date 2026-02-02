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
from types import MappingProxyType
from typing import Any, Literal, Self

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import BaseEnum, ExpressionFlavor
from pydantic import Field, field_validator, model_validator

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
    """

    NEQ = "!="
    EQ = "=="
    GE = ">="
    LE = "<="
    GT = ">"
    LT = "<"
    IN = "IN"


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
        """Map leaf 'operator' (YAML/kwargs) to unary_op; infer kind=leaf when missing."""
        if not isinstance(data, dict):
            return data
        # Only map operator -> unary_op for leaf clauses (never for composite)
        if data.get("kind") == "composite":
            return data
        if "operator" in data and isinstance(data["operator"], str):
            data = dict(data)
            data["unary_op"] = data.pop("operator")
            if data.get("kind") is None:
                data["kind"] = "leaf"
        return data

    @model_validator(mode="after")
    def check_discriminated_shape(self) -> FilterExpression:
        """Enforce exactly one shape per kind."""
        if self.kind == "leaf":
            if self.operator is not None or self.deps:
                raise ValueError("leaf expression must not have operator or deps")
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
                result.append(FilterExpression.from_dict(item))
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
    def from_dict(cls, current: dict[str, Any] | list[Any]) -> Self:  # type: ignore[override]
        """Create a filter expression from a dictionary or list.

        Returns FilterExpression (leaf or composite). LSP-compliant: return type is Self.
        """
        if isinstance(current, list):
            if current[0] in ComparisonOperator:
                return cls.from_list(current)  # type: ignore[return-value]
            elif current[0] in LogicalOperator:
                return cls(kind="composite", operator=current[0], deps=current[1])
        elif isinstance(current, dict):
            k = list(current.keys())[0]
            if k in LogicalOperator:
                deps = [cls.from_dict(v) for v in current[k]]
                return cls(kind="composite", operator=LogicalOperator(k), deps=deps)
            else:
                # Leaf from dict: map YAML "operator" -> unary_op
                unary_op = current.get("operator")
                return cls(
                    kind="leaf",
                    cmp_operator=current.get("cmp_operator"),
                    value=current.get("value", []),
                    field=current.get("field"),
                    unary_op=unary_op,
                )
        raise ValueError(f"expected dict or list, got {type(current)}")

    def __call__(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.AQL,
        **kwargs,
    ) -> str | bool:
        """Render or evaluate the expression in the target language."""
        if self.kind == "leaf":
            return self._call_leaf(doc_name=doc_name, kind=kind, **kwargs)
        return self._call_composite(doc_name=doc_name, kind=kind, **kwargs)

    def _call_leaf(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.AQL,
        **kwargs,
    ) -> str | bool:
        if not self.value:
            logger.warning(f"for {self} value is not set : {self.value}")
        if kind == ExpressionFlavor.AQL:
            assert self.cmp_operator is not None
            return self._cast_arango(doc_name)
        elif kind == ExpressionFlavor.CYPHER:
            assert self.cmp_operator is not None
            return self._cast_cypher(doc_name)
        elif kind == ExpressionFlavor.GSQL:
            assert self.cmp_operator is not None
            if doc_name == "":
                field_types = kwargs.get("field_types")
                return self._cast_restpp(field_types=field_types)
            return self._cast_tigergraph(doc_name)
        elif kind == ExpressionFlavor.PYTHON:
            return self._cast_python(**kwargs)
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
            ExpressionFlavor.GSQL,
        ):
            return self._cast_generic(doc_name=doc_name, kind=kind)
        elif kind == ExpressionFlavor.PYTHON:
            return self._cast_python_composite(kind=kind, **kwargs)
        raise ValueError(f"kind {kind} not implemented")

    def _cast_value(self) -> str:
        value = f"{self.value[0]}" if len(self.value) == 1 else f"{self.value}"
        if len(self.value) == 1:
            if isinstance(self.value[0], str):
                escaped = self.value[0].replace("\\", "\\\\").replace('"', '\\"')
                value = f'"{escaped}"'
            elif self.value[0] is None:
                value = "null"
            else:
                value = f"{self.value[0]}"
        return value

    def _cast_arango(self, doc_name: str) -> str:
        const = self._cast_value()
        lemma = f"{self.cmp_operator} {const}"
        if self.unary_op is not None:
            lemma = f"{self.unary_op} {lemma}"
        if self.field is not None:
            lemma = f'{doc_name}["{self.field}"] {lemma}'
        return lemma

    def _cast_cypher(self, doc_name: str) -> str:
        const = self._cast_value()
        cmp_op = (
            "=" if self.cmp_operator == ComparisonOperator.EQ else self.cmp_operator
        )
        lemma = f"{cmp_op} {const}"
        if self.unary_op is not None:
            lemma = f"{self.unary_op} {lemma}"
        if self.field is not None:
            lemma = f"{doc_name}.{self.field} {lemma}"
        return lemma

    def _cast_tigergraph(self, doc_name: str) -> str:
        const = self._cast_value()
        cmp_op = (
            "==" if self.cmp_operator == ComparisonOperator.EQ else self.cmp_operator
        )
        lemma = f"{cmp_op} {const}"
        if self.unary_op is not None:
            lemma = f"{self.unary_op} {lemma}"
        if self.field is not None:
            lemma = f"{doc_name}.{self.field} {lemma}"
        return lemma

    def _cast_restpp(self, field_types: dict[str, Any] | None = None) -> str:
        if not self.field:
            return ""
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

    def _cast_python(self, **kwargs: Any) -> bool:
        if self.field is not None:
            field_val = kwargs.pop(self.field, None)
            if field_val is not None and self.unary_op is not None:
                foo = getattr(field_val, self.unary_op)
                return foo(self.value[0])
        return False

    def _cast_generic(self, doc_name: str, kind: ExpressionFlavor) -> str:
        assert self.operator is not None
        if len(self.deps) == 1:
            if self.operator == LogicalOperator.NOT:
                result = self.deps[0](kind=kind, doc_name=doc_name)
                if doc_name == "" and kind == ExpressionFlavor.GSQL:
                    return f"!{result}"
                return f"{self.operator} {result}"
            raise ValueError(
                f" length of deps = {len(self.deps)} but operator is not {LogicalOperator.NOT}"
            )
        deps_str = [dep(kind=kind, doc_name=doc_name) for dep in self.deps]
        # __call__ returns str | bool; join expects str
        deps_str_cast: list[str] = [str(x) for x in deps_str]
        if doc_name == "" and kind == ExpressionFlavor.GSQL:
            if self.operator == LogicalOperator.AND:
                return " && ".join(deps_str_cast)
            if self.operator == LogicalOperator.OR:
                return " || ".join(deps_str_cast)
        return f" {self.operator} ".join(deps_str_cast)

    def _cast_python_composite(self, kind: ExpressionFlavor, **kwargs: Any) -> bool:
        assert self.operator is not None
        if len(self.deps) == 1:
            if self.operator == LogicalOperator.NOT:
                return not self.deps[0](kind=kind, **kwargs)
            raise ValueError(
                f" length of deps = {len(self.deps)} but operator is not {LogicalOperator.NOT}"
            )
        return OperatorMapping[self.operator](
            [dep(kind=kind, **kwargs) for dep in self.deps]
        )
