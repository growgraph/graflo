"""Declarative SQL-like view specifications.

Provides SelectSpec for describing queries in a structured way, similar to
FilterExpression. Supports type_lookup shorthand for edge tables where
source/target types come from lookup table(s) via FK joins, with optional
per-side table / join key / discriminator column. Asymmetric edges (e.g. one
static vertex type from EdgeRouterConfig only) use kind="select".
"""

from __future__ import annotations

import re
from typing import Any, Literal, Self

from pydantic import Field, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.filter.onto import FilterExpression
from graflo.onto import ExpressionFlavor
from graflo.architecture.contract.bindings import JoinClause

# Unquoted SQL identifiers (column / alias) for ergonomic select items.
_SIMPLE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\Z")


def _collect_join_aliases(joins: list[JoinClause | dict[str, Any]]) -> set[str]:
    aliases: set[str] = set()
    for j in joins:
        jc = JoinClause.model_validate(j) if isinstance(j, dict) else j
        aliases.add(jc.alias or jc.table)
    return aliases


def _sql_qualify_base_column(col: str, base_alias: str | None) -> str:
    if base_alias:
        return f'{base_alias}."{col}"'
    return f'"{col}"'


def _render_select_string(item: str, base_alias: str | None) -> str:
    if item == "*":
        return "*"
    if _SIMPLE_IDENT.match(item):
        return _sql_qualify_base_column(item, base_alias)
    return item


def _render_select_dict(
    item: dict[str, Any],
    base_alias: str | None,
    join_aliases: set[str],
) -> str:
    if "from_join" in item:
        alias = item["from_join"]
        if not isinstance(alias, str):
            raise ValueError("from_join select item requires string 'from_join' alias")
        if alias not in join_aliases:
            raise ValueError(
                f"select from_join alias {alias!r} is not among join aliases "
                f"{sorted(join_aliases)}"
            )
        col = item.get("column")
        if not col or not isinstance(col, str):
            raise ValueError("from_join select item requires string 'column'")
        expr = f'{alias}."{col}"'
        out = item.get("as") or item.get("alias")
        if out is not None:
            if not isinstance(out, str):
                raise ValueError("select output alias must be a string")
            return f"{expr} AS {out}"
        return expr
    if "base" in item:
        col = item["base"]
        if not isinstance(col, str):
            raise ValueError("base select item requires string 'base' column name")
        expr = _sql_qualify_base_column(col, base_alias)
        out = item.get("as") or item.get("alias")
        if out is not None:
            if not isinstance(out, str):
                raise ValueError("select output alias must be a string")
            return f"{expr} AS {out}"
        return expr
    expr = item.get("expr", "")
    if not isinstance(expr, str):
        raise ValueError("expr select item requires string 'expr'")
    out = item.get("as") or item.get("alias")
    if out is not None:
        if not isinstance(out, str):
            raise ValueError("select output alias must be a string")
        return f"{expr} AS {out}"
    return expr


class SelectSpec(ConfigBaseModel):
    """Declarative view specification emulating SQL SELECT structure.

    Alternative to TableConnector's table_name + joins + filters.
    Supports from_dict() for YAML/JSON loading (like FilterExpression).

    Attributes:
        kind: "select" for full spec, "type_lookup" for shorthand
        from_: Base table (used when kind="select")
        joins: JOIN clauses (used when kind="select")
        select: SELECT list (used when kind="select"). Each entry may be:
            - A string: ``*``, a raw SQL fragment (e.g. expressions), or a simple
              identifier that names a **base-table column** (auto-qualified with
              the base alias ``r`` when joins are present).
            - A dict ``{expr, alias}`` (legacy SQL expression).
            - A dict ``{base, as?}`` for a base-table column (optional output alias).
            - A dict ``{from_join, column, as?}`` for a column from a joined table
              (``from_join`` is the join alias from ``joins``).
        where: WHERE clause, reuses FilterExpression
        table: Lookup table (type_lookup only)
        identity: Identity column in lookup table (type_lookup only)
        type_column: Type discriminator column (type_lookup only)
        source: FK column on base table for source (type_lookup only)
        target: FK column on base table for target (type_lookup only)
        relation: Relation column in base table (type_lookup only, optional)
        source_table: Lookup table for the source side (defaults to table).
        target_table: Lookup table for the target side (defaults to table).
        source_identity: Join column on the source lookup (defaults to identity).
        target_identity: Join column on the target lookup (defaults to identity).
        source_type_column: Type discriminator on source lookup (defaults to
            type_column).
        target_type_column: Type discriminator on target lookup (defaults to
            type_column).
    """

    kind: Literal["select", "type_lookup"] = "select"

    # Full select form
    from_: str | None = Field(default=None, validation_alias="from")
    joins: list[JoinClause | dict[str, Any]] = Field(default_factory=list)
    select: list[str | dict[str, Any]] = Field(default_factory=lambda: ["*"])
    where: FilterExpression | dict[str, Any] | None = None

    # Type-lookup shorthand
    table: str | None = None
    identity: str | None = None
    type_column: str | None = None
    source: str | None = None
    target: str | None = None
    relation: str | None = None
    source_table: str | None = None
    target_table: str | None = None
    source_identity: str | None = None
    target_identity: str | None = None
    source_type_column: str | None = None
    target_type_column: str | None = None

    @model_validator(mode="after")
    def _validate_type_lookup_side_options(self) -> Self:
        if self.kind != "type_lookup":
            return self

        def eff_source_lookup() -> tuple[str | None, str | None, str | None]:
            t = self.source_table if self.source_table is not None else self.table
            i = (
                self.source_identity
                if self.source_identity is not None
                else self.identity
            )
            c = (
                self.source_type_column
                if self.source_type_column is not None
                else self.type_column
            )
            return (t, i, c)

        def eff_target_lookup() -> tuple[str | None, str | None, str | None]:
            t = self.target_table if self.target_table is not None else self.table
            i = (
                self.target_identity
                if self.target_identity is not None
                else self.identity
            )
            c = (
                self.target_type_column
                if self.target_type_column is not None
                else self.type_column
            )
            return (t, i, c)

        t, i, c = eff_source_lookup()
        if not all([t, i, c]):
            raise ValueError(
                "type_lookup requires table, identity, type_column (or "
                "source_table / source_identity / source_type_column) for the "
                "source lookup join"
            )
        t, i, c = eff_target_lookup()
        if not all([t, i, c]):
            raise ValueError(
                "type_lookup requires table, identity, type_column (or "
                "target_table / target_identity / target_type_column) for the "
                "target lookup join"
            )
        return self

    def build_sql(
        self,
        schema: str,
        base_table: str,
    ) -> str:
        """Build SQL SELECT query.

        Args:
            schema: Schema name (e.g. "public")
            base_table: Base table name (from TableConnector.table_name)

        Returns:
            Complete SQL query string
        """
        if self.kind == "type_lookup":
            return self._build_type_lookup_sql(schema, base_table)
        return self._build_select_sql(schema, base_table)

    def _build_type_lookup_sql(self, schema: str, base_table: str) -> str:
        """Expand type_lookup shorthand to full SQL."""
        if not self.source or not self.target:
            raise ValueError("type_lookup requires source and target column names")

        src_fk = self.source
        tgt_fk = self.target
        rel_col = self.relation

        src_tbl = self.source_table if self.source_table is not None else self.table
        src_ident = (
            self.source_identity if self.source_identity is not None else self.identity
        )
        src_type_col = (
            self.source_type_column
            if self.source_type_column is not None
            else self.type_column
        )

        tgt_tbl = self.target_table if self.target_table is not None else self.table
        tgt_ident = (
            self.target_identity if self.target_identity is not None else self.identity
        )
        tgt_type_col = (
            self.target_type_column
            if self.target_type_column is not None
            else self.type_column
        )

        base_ref = f'"{schema}"."{base_table}"'

        assert src_type_col is not None and tgt_type_col is not None
        select_parts: list[str] = [
            f'r."{src_fk}" AS source_id',
            f's."{src_type_col}" AS source_type',
            f'r."{tgt_fk}" AS target_id',
            f't."{tgt_type_col}" AS target_type',
        ]

        if rel_col:
            select_parts.append(f'r."{rel_col}" AS relation')
        select_clause = ", ".join(select_parts)

        assert src_tbl is not None and src_ident is not None
        assert tgt_tbl is not None and tgt_ident is not None
        s_ref = f'"{schema}"."{src_tbl}"'
        t_ref = f'"{schema}"."{tgt_tbl}"'
        from_clause = (
            f"{base_ref} r "
            f'LEFT JOIN {s_ref} s ON r."{src_fk}" = s."{src_ident}" '
            f'LEFT JOIN {t_ref} t ON r."{tgt_fk}" = t."{tgt_ident}"'
        )

        where_clause = f's."{src_ident}" IS NOT NULL AND t."{tgt_ident}" IS NOT NULL'
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
        join_aliases = _collect_join_aliases(self.joins)
        select_parts: list[str] = []
        for item in self.select:
            if isinstance(item, str):
                select_parts.append(_render_select_string(item, base_alias))
            elif isinstance(item, dict):
                select_parts.append(_render_select_dict(item, base_alias, join_aliases))
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
    def concat_select_parts(cls, head: Self, *tail: Self) -> Self:
        """Merge ``kind="select"`` join/select fragments into one spec.

        ``head`` may set ``from_`` and ``where``. Each ``tail`` fragment must omit
        ``from_`` and ``where`` so the logical base table and filters stay on
        ``head``. ``joins`` and ``select`` are concatenated in order.

        Omitting ``from`` on the merged spec is fine: :meth:`build_sql` uses
        ``TableConnector.table_name`` as the base when ``from_`` is unset.

        This is the structured equivalent of listing two join/select blocks in
        one YAML ``view``; see ``docs/concepts/table_connector_views.md`` for
        YAML anchor patterns.
        """
        if head.kind != "select":
            raise ValueError("head must use kind='select'")
        for part in tail:
            if part.kind != "select":
                raise ValueError("all fragments must use kind='select'")
            if part.from_ is not None:
                raise ValueError(
                    "tail SelectSpec fragments must omit `from` "
                    "(base table is TableConnector.table_name at build_sql time)"
                )
            if part.where is not None:
                raise ValueError(
                    "tail fragments must omit `where`; set it on `head` only"
                )
        merged_joins: list[JoinClause | dict[str, Any]] = list(head.joins)
        merged_select: list[str | dict[str, Any]] = list(head.select)
        for part in tail:
            merged_joins.extend(part.joins)
            merged_select.extend(part.select)
        return cls(
            kind="select",
            from_=head.from_,
            joins=merged_joins,
            select=merged_select if merged_select else ["*"],
            where=head.where,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any] | list[Any]) -> Self:
        """Create SelectSpec from dictionary (YAML/JSON friendly).

        Supports:
        - kind="type_lookup": table, identity, type_column, source, target, relation,
          optional per-side source_* / target_* lookup overrides
        - kind="select": from, joins, select (strings, base/from_join dicts, or
          legacy expr/alias dicts), where
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
