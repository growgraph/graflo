"""Declarative SQL-like view specifications.

Provides SelectSpec for describing queries in a structured way, similar to
FilterExpression. Supports type_lookup shorthand for edge tables where
source/target types come from a lookup table via FK joins.
"""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.filter.onto import FilterExpression
from graflo.onto import ExpressionFlavor
from graflo.util.onto import JoinClause


class SelectSpec(ConfigBaseModel):
    """Declarative view specification emulating SQL SELECT structure.

    Alternative to TablePattern's table_name + joins + filters.
    Supports from_dict() for YAML/JSON loading (like FilterExpression).

    Attributes:
        kind: "select" for full spec, "type_lookup" for shorthand
        from_: Base table (used when kind="select")
        joins: JOIN clauses (used when kind="select")
        select: SELECT list (used when kind="select")
        where: WHERE clause, reuses FilterExpression
        table: Lookup table (type_lookup only)
        identity: Identity column in lookup table (type_lookup only)
        type_column: Type discriminator column (type_lookup only)
        source: FK column on base table for source (type_lookup only)
        target: FK column on base table for target (type_lookup only)
        relation: Relation column in base table (type_lookup only, optional)
    """

    kind: Literal["select", "type_lookup"] = "select"

    # Full select form
    from_: str | None = Field(default=None, validation_alias="from")
    joins: list[JoinClause | dict[str, Any]] = Field(default_factory=list)
    select: list[str] | list[dict[str, Any]] = Field(default_factory=lambda: ["*"])
    where: FilterExpression | dict[str, Any] | None = None

    # Type-lookup shorthand
    table: str | None = None
    identity: str | None = None
    type_column: str | None = None
    source: str | None = None
    target: str | None = None
    relation: str | None = None

    def build_sql(
        self,
        schema: str,
        base_table: str,
    ) -> str:
        """Build SQL SELECT query.

        Args:
            schema: Schema name (e.g. "public")
            base_table: Base table name (from TablePattern.table_name)

        Returns:
            Complete SQL query string
        """
        if self.kind == "type_lookup":
            return self._build_type_lookup_sql(schema, base_table)
        return self._build_select_sql(schema, base_table)

    def _build_type_lookup_sql(self, schema: str, base_table: str) -> str:
        """Expand type_lookup shorthand to full SQL."""
        if not all(
            [self.table, self.identity, self.type_column, self.source, self.target]
        ):
            raise ValueError(
                "type_lookup requires table, identity, type_column, source, target"
            )
        lookup = self.table
        ident = self.identity
        type_col = self.type_column
        src_fk = self.source
        tgt_fk = self.target
        rel_col = self.relation

        base_ref = f'"{schema}"."{base_table}"'
        lookup_ref = f'"{schema}"."{lookup}"'

        select_parts = [
            f's."{ident}" AS source_id',
            f's."{type_col}" AS source_type',
            f't."{ident}" AS target_id',
            f't."{type_col}" AS target_type',
        ]
        if rel_col:
            select_parts.append(f'r."{rel_col}" AS relation')
        select_clause = ", ".join(select_parts)

        from_clause = f"{base_ref} r"
        join_s = f'LEFT JOIN {lookup_ref} s ON r."{src_fk}" = s."{ident}"'
        join_t = f'LEFT JOIN {lookup_ref} t ON r."{tgt_fk}" = t."{ident}"'
        from_clause += f" {join_s} {join_t}"

        where_parts = [
            f's."{ident}" IS NOT NULL',
            f't."{ident}" IS NOT NULL',
        ]
        where_clause = " AND ".join(where_parts)

        return f"SELECT {select_clause} FROM {from_clause} WHERE {where_clause}"

    def _build_select_sql(self, schema: str, base_table: str) -> str:
        """Build SQL from full select spec."""
        from_table = self.from_ or base_table
        base_ref = f'"{schema}"."{from_table}"'
        base_alias = "r" if self.joins else None
        if base_alias:
            base_ref_aliased = f"{base_ref} {base_alias}"
        else:
            base_ref_aliased = base_ref

        # SELECT
        select_parts: list[str] = []
        for item in self.select:
            if isinstance(item, str):
                select_parts.append(item)
            elif isinstance(item, dict):
                expr = item.get("expr", "")
                alias = item.get("alias")
                if alias:
                    select_parts.append(f"{expr} AS {alias}")
                else:
                    select_parts.append(expr)
        select_clause = ", ".join(select_parts) if select_parts else "*"

        # FROM + JOINs
        from_clause = base_ref_aliased
        for j in self.joins:
            jc = JoinClause.model_validate(j) if isinstance(j, dict) else j
            jc_schema = jc.schema_name or schema
            alias = jc.alias or jc.table
            join_ref = f'"{jc_schema}"."{jc.table}"'
            left_col = (
                f'{base_alias}."{jc.on_self}"' if base_alias else f'"{jc.on_self}"'
            )
            right_col = f'{alias}."{jc.on_other}"'
            from_clause += (
                f" {jc.join_type} JOIN {join_ref} {alias} ON {left_col} = {right_col}"
            )

        query = f"SELECT {select_clause} FROM {from_clause}"

        # WHERE
        if self.where:
            we = (
                FilterExpression.from_dict(self.where)
                if isinstance(self.where, dict)
                else self.where
            )
            where_str = we(kind=ExpressionFlavor.SQL)
            if where_str:
                query += f" WHERE {where_str}"

        return query

    @classmethod
    def from_dict(cls, data: dict[str, Any] | list[Any]) -> Self:
        """Create SelectSpec from dictionary (YAML/JSON friendly).

        Supports:
        - kind="type_lookup": table, identity, type_column, source, target, relation
        - kind="select": from, joins, select, where
        """
        if isinstance(data, list):
            return cls.model_validate(data)
        data = dict(data)
        kind = data.pop("kind", "select")
        if kind == "type_lookup":
            return cls(
                kind="type_lookup",
                **{k: v for k, v in data.items() if k != "from" and v is not None},
            )
        # Normalize "from" -> from_
        if "from" in data:
            data["from_"] = data.pop("from")
        return cls(kind="select", **data)
