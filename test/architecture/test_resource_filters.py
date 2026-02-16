"""Unit tests for resource-level query enhancements.

Tests cover:
- FilterExpression IS_NULL / IS_NOT_NULL across all flavours
- TablePattern.build_query() with joins and filters
- Auto-JOIN generation helper (enrich_edge_pattern_with_joins)
"""

from __future__ import annotations


from graflo.filter.onto import ComparisonOperator, FilterExpression, LogicalOperator
from graflo.onto import ExpressionFlavor
from graflo.util.onto import JoinClause, TablePattern


# ---------------------------------------------------------------
# Phase 1: IS_NULL / IS_NOT_NULL operators
# ---------------------------------------------------------------


class TestIsNullIsNotNull:
    """FilterExpression rendering of IS_NULL / IS_NOT_NULL across flavours."""

    def _leaf(self, field: str, op: ComparisonOperator) -> FilterExpression:
        return FilterExpression(kind="leaf", field=field, cmp_operator=op)

    # --- SQL ---

    def test_is_null_sql(self):
        expr = self._leaf("class_name", ComparisonOperator.IS_NULL)
        assert expr(kind=ExpressionFlavor.SQL) == '"class_name" IS NULL'

    def test_is_not_null_sql(self):
        expr = self._leaf("class_name", ComparisonOperator.IS_NOT_NULL)
        assert expr(kind=ExpressionFlavor.SQL) == '"class_name" IS NOT NULL'

    def test_is_not_null_sql_aliased_field(self):
        """Dotted field ``s.id`` should render as ``s."id" IS NOT NULL``."""
        expr = self._leaf("s.id", ComparisonOperator.IS_NOT_NULL)
        assert expr(kind=ExpressionFlavor.SQL) == 's."id" IS NOT NULL'

    # --- AQL ---

    def test_is_null_aql(self):
        expr = self._leaf("name", ComparisonOperator.IS_NULL)
        assert expr(doc_name="d", kind=ExpressionFlavor.AQL) == 'd["name"] == null'

    def test_is_not_null_aql(self):
        expr = self._leaf("name", ComparisonOperator.IS_NOT_NULL)
        assert expr(doc_name="d", kind=ExpressionFlavor.AQL) == 'd["name"] != null'

    # --- Cypher ---

    def test_is_null_cypher(self):
        expr = self._leaf("age", ComparisonOperator.IS_NULL)
        assert expr(doc_name="n", kind=ExpressionFlavor.CYPHER) == "n.age IS NULL"

    def test_is_not_null_cypher(self):
        expr = self._leaf("age", ComparisonOperator.IS_NOT_NULL)
        assert expr(doc_name="n", kind=ExpressionFlavor.CYPHER) == "n.age IS NOT NULL"

    # --- GSQL (TigerGraph) ---

    def test_is_null_gsql(self):
        expr = self._leaf("status", ComparisonOperator.IS_NULL)
        assert expr(doc_name="v", kind=ExpressionFlavor.GSQL) == "v.status IS NULL"

    def test_is_not_null_gsql(self):
        expr = self._leaf("status", ComparisonOperator.IS_NOT_NULL)
        assert expr(doc_name="v", kind=ExpressionFlavor.GSQL) == "v.status IS NOT NULL"

    # --- REST++ (TigerGraph with empty doc_name) ---

    def test_is_null_restpp(self):
        expr = self._leaf("x", ComparisonOperator.IS_NULL)
        result = expr(doc_name="", kind=ExpressionFlavor.GSQL)
        assert result == 'x=""'

    def test_is_not_null_restpp(self):
        expr = self._leaf("x", ComparisonOperator.IS_NOT_NULL)
        result = expr(doc_name="", kind=ExpressionFlavor.GSQL)
        assert result == 'x!=""'

    # --- Python ---

    def test_is_null_python_true(self):
        expr = self._leaf("col", ComparisonOperator.IS_NULL)
        assert expr(kind=ExpressionFlavor.PYTHON, col=None) is True

    def test_is_null_python_false(self):
        expr = self._leaf("col", ComparisonOperator.IS_NULL)
        assert expr(kind=ExpressionFlavor.PYTHON, col="val") is False

    def test_is_not_null_python_true(self):
        expr = self._leaf("col", ComparisonOperator.IS_NOT_NULL)
        assert expr(kind=ExpressionFlavor.PYTHON, col="val") is True

    def test_is_not_null_python_false(self):
        expr = self._leaf("col", ComparisonOperator.IS_NOT_NULL)
        assert expr(kind=ExpressionFlavor.PYTHON, col=None) is False

    # --- Composite with IS_NOT_NULL ---

    def test_and_with_is_not_null_sql(self):
        expr = FilterExpression(
            kind="composite",
            operator=LogicalOperator.AND,
            deps=[
                self._leaf("s.id", ComparisonOperator.IS_NOT_NULL),
                self._leaf("t.id", ComparisonOperator.IS_NOT_NULL),
            ],
        )
        result = expr(kind=ExpressionFlavor.SQL)
        assert isinstance(result, str)
        assert 's."id" IS NOT NULL' in result
        assert 't."id" IS NOT NULL' in result
        assert " AND " in result

    # --- value list is cleared for null ops ---

    def test_value_cleared(self):
        expr = FilterExpression(
            kind="leaf",
            field="f",
            cmp_operator=ComparisonOperator.IS_NULL,
            value=["should_be_cleared"],
        )
        assert expr.value == []


# ---------------------------------------------------------------
# Phase 2: TablePattern.build_query
# ---------------------------------------------------------------


class TestTablePatternBuildQuery:
    """TablePattern.build_query() generates correct SQL."""

    def test_simple_select_star(self):
        tp = TablePattern(table_name="users")
        q = tp.build_query("public")
        assert q == 'SELECT * FROM "public"."users"'

    def test_with_date_filter(self):
        tp = TablePattern(
            table_name="events",
            date_field="created_at",
            date_filter="> '2020-01-01'",
        )
        q = tp.build_query("public")
        assert 'WHERE "created_at" >' in q

    def test_with_filter_expression(self):
        f = FilterExpression(
            kind="leaf",
            field="class_name",
            cmp_operator=ComparisonOperator.EQ,
            value=["server"],
        )
        tp = TablePattern(table_name="classes", filters=[f])
        q = tp.build_query("myschema")
        assert "WHERE" in q
        assert "\"class_name\" = 'server'" in q

    def test_with_multiple_filters(self):
        f1 = FilterExpression(
            kind="leaf",
            field="status",
            cmp_operator=ComparisonOperator.EQ,
            value=["active"],
        )
        f2 = FilterExpression(
            kind="leaf",
            field="age",
            cmp_operator=ComparisonOperator.GE,
            value=[18],
        )
        tp = TablePattern(table_name="people", filters=[f1, f2])
        q = tp.build_query("public")
        assert "AND" in q
        assert '"status"' in q
        assert '"age"' in q

    def test_with_single_join(self):
        jc = JoinClause(
            table="addresses",
            alias="a",
            on_self="address_id",
            on_other="id",
        )
        tp = TablePattern(table_name="users", joins=[jc])
        q = tp.build_query("public")
        assert "LEFT JOIN" in q
        assert '"public"."addresses" a' in q
        assert 'r."address_id" = a."id"' in q
        # base table aliased as 'r'
        assert "r.*" in q

    def test_with_two_joins_same_table(self):
        """CMDB-style: two joins to same table with different aliases."""
        jc_s = JoinClause(
            table="classes",
            alias="s",
            on_self="parent",
            on_other="id",
        )
        jc_t = JoinClause(
            table="classes",
            alias="t",
            on_self="child",
            on_other="id",
        )
        f1 = FilterExpression(
            kind="leaf",
            field="s.id",
            cmp_operator=ComparisonOperator.IS_NOT_NULL,
        )
        f2 = FilterExpression(
            kind="leaf",
            field="t.id",
            cmp_operator=ComparisonOperator.IS_NOT_NULL,
        )
        tp = TablePattern(
            table_name="cmdb_rel_ci",
            joins=[jc_s, jc_t],
            filters=[f1, f2],
        )
        q = tp.build_query("sn")
        # Both JOINs present
        assert '"sn"."classes" s' in q
        assert '"sn"."classes" t' in q
        assert 'r."parent" = s."id"' in q
        assert 'r."child" = t."id"' in q
        # IS NOT NULL filters
        assert 's."id" IS NOT NULL' in q
        assert 't."id" IS NOT NULL' in q

    def test_join_with_select_fields(self):
        jc = JoinClause(
            table="classes",
            alias="s",
            on_self="parent",
            on_other="id",
            select_fields=["id", "class_name"],
        )
        tp = TablePattern(table_name="rel", joins=[jc])
        q = tp.build_query("public")
        assert 's."id" AS "s__id"' in q
        assert 's."class_name" AS "s__class_name"' in q

    def test_explicit_select_columns(self):
        tp = TablePattern(
            table_name="t",
            select_columns=["a", "b"],
        )
        q = tp.build_query("public")
        assert q.startswith("SELECT a, b FROM")

    def test_schema_defaults_to_public(self):
        tp = TablePattern(table_name="t")
        q = tp.build_query()
        assert '"public"."t"' in q


# ---------------------------------------------------------------
# Phase 3: Auto-JOIN generation
# ---------------------------------------------------------------


class TestAutoJoin:
    """enrich_edge_pattern_with_joins adds JoinClauses from edge defs."""

    def _make_schema_and_patterns(self):
        """Build a minimal Schema + Patterns for the CMDB-like scenario."""
        from graflo.architecture.schema import Schema
        from graflo.util.onto import Patterns

        schema = Schema.model_validate(
            {
                "general": {"name": "test", "version": "0.0.1"},
                "vertex_config": {
                    "vertices": [
                        {"name": "server", "fields": ["id", "class_name"]},
                        {"name": "database", "fields": ["id", "class_name"]},
                    ],
                },
                "edge_config": {
                    "edges": [
                        {"source": "server", "target": "database"},
                    ],
                },
                "resources": [
                    {
                        "resource_name": "cmdb_relations",
                        "pipeline": [
                            {
                                "edge": {
                                    "from": "server",
                                    "to": "database",
                                    "match_source": "parent",
                                    "match_target": "child",
                                }
                            }
                        ],
                    }
                ],
            }
        )

        patterns = Patterns(
            table_patterns={
                "server": TablePattern(table_name="classes", schema_name="sn"),
                "database": TablePattern(table_name="classes", schema_name="sn"),
                "cmdb_relations": TablePattern(
                    table_name="cmdb_rel_ci", schema_name="sn"
                ),
            },
        )
        return schema, patterns

    def test_enrichment_adds_joins(self):
        from graflo.hq.auto_join import enrich_edge_pattern_with_joins

        schema, patterns = self._make_schema_and_patterns()
        resource = schema.fetch_resource("cmdb_relations")
        pattern = patterns.table_patterns["cmdb_relations"]

        enrich_edge_pattern_with_joins(
            resource=resource,
            pattern=pattern,
            patterns=patterns,
            vertex_config=schema.vertex_config,
        )

        assert len(pattern.joins) == 2
        aliases = {j.alias for j in pattern.joins}
        assert aliases == {"s", "t"}
        # The on_self fields come from edge match_source / match_target
        on_self_cols = {j.on_self for j in pattern.joins}
        assert on_self_cols == {"parent", "child"}

    def test_enrichment_adds_is_not_null_filters(self):
        from graflo.hq.auto_join import enrich_edge_pattern_with_joins

        schema, patterns = self._make_schema_and_patterns()
        resource = schema.fetch_resource("cmdb_relations")
        pattern = patterns.table_patterns["cmdb_relations"]

        enrich_edge_pattern_with_joins(
            resource=resource,
            pattern=pattern,
            patterns=patterns,
            vertex_config=schema.vertex_config,
        )

        assert len(pattern.filters) == 2
        rendered = [f(kind=ExpressionFlavor.SQL) for f in pattern.filters]
        assert 's."id" IS NOT NULL' in rendered
        assert 't."id" IS NOT NULL' in rendered

    def test_enrichment_noop_when_joins_already_set(self):
        from graflo.hq.auto_join import enrich_edge_pattern_with_joins

        schema, patterns = self._make_schema_and_patterns()
        resource = schema.fetch_resource("cmdb_relations")
        pattern = patterns.table_patterns["cmdb_relations"]
        pattern.joins = [JoinClause(table="x", alias="x", on_self="a", on_other="b")]

        enrich_edge_pattern_with_joins(
            resource=resource,
            pattern=pattern,
            patterns=patterns,
            vertex_config=schema.vertex_config,
        )

        # Should not have modified the existing join
        assert len(pattern.joins) == 1
        assert pattern.joins[0].table == "x"

    def test_full_query_after_enrichment(self):
        from graflo.hq.auto_join import enrich_edge_pattern_with_joins

        schema, patterns = self._make_schema_and_patterns()
        resource = schema.fetch_resource("cmdb_relations")
        pattern = patterns.table_patterns["cmdb_relations"]

        enrich_edge_pattern_with_joins(
            resource=resource,
            pattern=pattern,
            patterns=patterns,
            vertex_config=schema.vertex_config,
        )

        q = pattern.build_query("sn")
        assert "LEFT JOIN" in q
        assert "IS NOT NULL" in q
        assert '"sn"."cmdb_rel_ci"' in q
