import pytest
import yaml

from graflo.filter.onto import (
    ComparisonOperator,
    FilterExpression,
    LogicalOperator,
)
from graflo.onto import ExpressionFlavor
from graflo import VertexConfig


@pytest.fixture()
def clause_open():
    s = yaml.safe_load(
        """
        field: name
        operator: __eq__
        value: Open
    """
    )
    return s


@pytest.fixture()
def clause_close():
    s = yaml.safe_load(
        """
        field: name
        operator: __eq__
        value: Close
    """
    )
    return s


@pytest.fixture()
def clause_volume():
    s = yaml.safe_load(
        """
        field: name
        operator: __ne__
        value: Volume
    """
    )
    return s


@pytest.fixture()
def clause_b():
    s = yaml.safe_load(
        """
        field: value
        operator: __gt__
        value: 0
    """
    )
    return s


@pytest.fixture()
def clause_a(clause_open, clause_b):
    s = {LogicalOperator.AND: [clause_open, clause_b]}
    return s


@pytest.fixture()
def clause_ab(clause_open, clause_close, clause_b):
    s = {
        LogicalOperator.OR: [
            {LogicalOperator.AND: [clause_open, clause_b]},
            {LogicalOperator.AND: [clause_close, clause_b]},
        ]
    }
    return s


@pytest.fixture()
def filter_implication(clause_open, clause_b):
    s = {LogicalOperator.IMPLICATION: [clause_open, clause_b]}
    return s


def test_python_clause(clause_open):
    lc = FilterExpression(**clause_open)  # kind=leaf inferred from operator (str)
    doc = {"name": "Open"}
    assert lc(**doc, kind=ExpressionFlavor.PYTHON)


def test_condition_b(clause_b):
    m = FilterExpression(**clause_b)  # kind=leaf inferred from operator (str)
    doc = {"value": -1}
    assert m(value=1, kind=ExpressionFlavor.PYTHON)
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)


def test_clause_a(clause_a):
    m = FilterExpression.from_dict(clause_a)

    doc = {"name": "Open", "value": 5.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Open", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)


def test_clause_ab(clause_ab):
    m = FilterExpression.from_dict(clause_ab)

    doc = {"name": "Open", "value": 5.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Open", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Close", "value": 5.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Close", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)


def test_filter_implication(filter_implication):
    m = FilterExpression.from_dict(filter_implication)

    doc = {"name": "Open", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Close", "value": -1.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)


def test_filter_neq(clause_volume):
    m = FilterExpression.from_dict(clause_volume)

    doc = {"name": "Open", "value": -1.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Volume", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)


def test_filter_expression_sql_leaf():
    """FilterExpression renders leaf to SQL WHERE fragment (ExpressionFlavor.SQL)."""
    leaf = FilterExpression(
        kind="leaf",
        field="created_at",
        cmp_operator=ComparisonOperator.GE,
        value=["2020-01-01T00:00:00"],
    )
    out = leaf(kind=ExpressionFlavor.SQL)
    assert out == "\"created_at\" >= '2020-01-01T00:00:00'"


def test_filter_expression_sql_composite_and():
    """FilterExpression AND composite renders to SQL with AND."""
    ge = FilterExpression(
        kind="leaf",
        field="dt",
        cmp_operator=ComparisonOperator.GE,
        value=["2020-01-01"],
    )
    lt = FilterExpression(
        kind="leaf",
        field="dt",
        cmp_operator=ComparisonOperator.LT,
        value=["2020-12-31"],
    )
    expr = FilterExpression(
        kind="composite",
        operator=LogicalOperator.AND,
        deps=[ge, lt],
    )
    out = expr(kind=ExpressionFlavor.SQL)
    assert isinstance(out, str)
    assert "\"dt\" >= '2020-01-01'" in out
    assert "\"dt\" < '2020-12-31'" in out
    assert " AND " in out


# ---------------------------------------------------------------------------
# Tests for 'foo' key in YAML filters (dunder-method shorthand)
# ---------------------------------------------------------------------------


@pytest.fixture()
def foo_clause_open():
    return yaml.safe_load(
        """
        field: name
        foo: __eq__
        value: Open
    """
    )


@pytest.fixture()
def foo_clause_close():
    return yaml.safe_load(
        """
        field: name
        foo: __eq__
        value: Close
    """
    )


@pytest.fixture()
def foo_clause_positive_value():
    return yaml.safe_load(
        """
        field: value
        foo: __gt__
        value: 0
    """
    )


@pytest.fixture()
def foo_clause_volume():
    return yaml.safe_load(
        """
        field: name
        foo: __ne__
        value: Volume
    """
    )


def test_foo_leaf_infers_cmp_operator(foo_clause_open):
    """'foo: __eq__' should populate both unary_op and cmp_operator."""
    expr = FilterExpression.from_dict(foo_clause_open)
    assert expr.kind == "leaf"
    assert expr.unary_op == "__eq__"
    assert expr.cmp_operator == ComparisonOperator.EQ


def test_foo_leaf_python_evaluation(foo_clause_open):
    expr = FilterExpression.from_dict(foo_clause_open)
    assert expr(kind=ExpressionFlavor.PYTHON, name="Open")
    assert not expr(kind=ExpressionFlavor.PYTHON, name="Close")


def test_foo_leaf_aql_rendering(foo_clause_open):
    """A leaf built from 'foo' should also render correctly in AQL."""
    expr = FilterExpression.from_dict(foo_clause_open)
    out = expr(doc_name="doc", kind=ExpressionFlavor.AQL)
    assert isinstance(out, str)
    assert '"name"' in out
    assert '== "Open"' in out


def test_foo_neq_python(foo_clause_volume):
    expr = FilterExpression.from_dict(foo_clause_volume)
    assert expr.cmp_operator == ComparisonOperator.NEQ
    assert expr(kind=ExpressionFlavor.PYTHON, name="Open")
    assert not expr(kind=ExpressionFlavor.PYTHON, name="Volume")


def test_foo_gt_python(foo_clause_positive_value):
    expr = FilterExpression.from_dict(foo_clause_positive_value)
    assert expr.cmp_operator == ComparisonOperator.GT
    assert expr(kind=ExpressionFlavor.PYTHON, value=5)
    assert not expr(kind=ExpressionFlavor.PYTHON, value=-1)


def test_foo_implication_from_dict(foo_clause_open, foo_clause_positive_value):
    """IF_THEN composite built with 'foo'-style leaves evaluates correctly."""
    raw = {"if_then": [foo_clause_open, foo_clause_positive_value]}
    expr = FilterExpression.from_dict(raw)

    assert expr.kind == "composite"
    assert expr.operator == LogicalOperator.IMPLICATION

    # name=Open and value>0 -> pass
    assert expr(kind=ExpressionFlavor.PYTHON, name="Open", value=5.0)
    # name=Open and value<0 -> fail (consequent false)
    assert not expr(kind=ExpressionFlavor.PYTHON, name="Open", value=-1.0)
    # name=Close -> antecedent false, implication is True
    assert expr(kind=ExpressionFlavor.PYTHON, name="Close", value=-1.0)


def test_foo_ticker_yaml_filters_or():
    """End-to-end: parse the exact OR-based filter structure from ticker.yaml.

    With OR(IF_THEN(A,B), IF_THEN(C,D)) where A and C are mutually exclusive,
    at least one implication is vacuously true so the OR is always True.
    Only the second top-level filter (name != Volume) actually rejects docs.
    """
    raw_filters = yaml.safe_load(
        """
    - or:
        - if_then:
            - field: name
              foo: __eq__
              value: Open
            - field: value
              foo: __gt__
              value: 0
        - if_then:
            - field: name
              foo: __eq__
              value: Close
            - field: value
              foo: __gt__
              value: 0
    - field: name
      foo: __ne__
      value: Volume
    """
    )
    filters = [FilterExpression.from_dict(f) for f in raw_filters]
    assert len(filters) == 2

    def passes_all(doc: dict) -> bool:
        return all(f(kind=ExpressionFlavor.PYTHON, **doc) for f in filters)

    assert passes_all({"name": "Open", "value": 5.0})
    # OR vacuously passes even with negative value
    assert passes_all({"name": "Open", "value": -1.0})
    assert passes_all({"name": "Close", "value": 5.0})
    assert passes_all({"name": "Close", "value": -1.0})
    # Volume -> rejected by the __ne__ filter
    assert not passes_all({"name": "Volume", "value": 100})
    assert passes_all({"name": "High", "value": -3.0})


def test_foo_ticker_yaml_filters_and():
    """AND-based variant: AND(IF_THEN, IF_THEN) correctly enforces positive values."""
    raw_filters = yaml.safe_load(
        """
    - and:
        - if_then:
            - field: name
              foo: __eq__
              value: Open
            - field: value
              foo: __gt__
              value: 0
        - if_then:
            - field: name
              foo: __eq__
              value: Close
            - field: value
              foo: __gt__
              value: 0
    - field: name
      foo: __ne__
      value: Volume
    """
    )
    filters = [FilterExpression.from_dict(f) for f in raw_filters]
    assert len(filters) == 2

    def passes_all(doc: dict) -> bool:
        return all(f(kind=ExpressionFlavor.PYTHON, **doc) for f in filters)

    assert passes_all({"name": "Open", "value": 5.0})
    assert not passes_all({"name": "Open", "value": -1.0})
    assert passes_all({"name": "Close", "value": 5.0})
    assert not passes_all({"name": "Close", "value": -1.0})
    assert not passes_all({"name": "Volume", "value": 100})
    assert passes_all({"name": "High", "value": -3.0})


def test_foo_direct_construction():
    """FilterExpression.model_validate with 'foo' infers kind, cmp_operator, unary_op."""
    expr = FilterExpression.model_validate({"field": "x", "foo": "__le__", "value": 10})
    assert expr.kind == "leaf"
    assert expr.unary_op == "__le__"
    assert expr.cmp_operator == ComparisonOperator.LE
    assert expr(kind=ExpressionFlavor.PYTHON, x=5)
    assert not expr(kind=ExpressionFlavor.PYTHON, x=15)


# ---------------------------------------------------------------------------
# Integration: Vertex -> VertexConfig -> filter evaluation (actor-level coupling)
# ---------------------------------------------------------------------------


@pytest.fixture()
def vertex_config_with_filters():
    """VertexConfig built from YAML with foo-style filters (AND variant)."""
    return yaml.safe_load(
        """
    vertices:
    -   name: feature
        fields:
        -   name
        -   value
        filters:
        -   and:
            -   if_then:
                -   field: name
                    foo: __eq__
                    value: Open
                -   field: value
                    foo: __gt__
                    value: 0
            -   if_then:
                -   field: name
                    foo: __eq__
                    value: Close
                -   field: value
                    foo: __gt__
                    value: 0
        -   field: name
            foo: __ne__
            value: Volume
    """
    )


@pytest.fixture()
def sample_vertex_docs():
    return [
        {"name": "Open", "value": 5.0},
        {"name": "Open", "value": -1.0},
        {"name": "Close", "value": 3.0},
        {"name": "Close", "value": -2.0},
        {"name": "Volume", "value": 100},
        {"name": "High", "value": -3.0},
    ]


def _apply_vertex_filters(
    vertex_config: VertexConfig,
    vertex_name: str,
    docs: list[dict],
) -> list[dict]:
    """Replicate the filtering logic from VertexActor._filter_and_aggregate_vertex_docs."""
    filters = vertex_config.filters(vertex_name)
    return [
        d for d in docs if all(f(kind=ExpressionFlavor.PYTHON, **d) for f in filters)
    ]


def test_vertex_config_parses_foo_filters(vertex_config_with_filters):
    """VertexConfig correctly parses foo-style filters from YAML into FilterExpressions."""
    from graflo.architecture.vertex import VertexConfig

    vc = VertexConfig.model_validate(vertex_config_with_filters)
    filters = vc.filters("feature")
    assert len(filters) == 2
    assert filters[0].kind == "composite"
    assert filters[0].operator == LogicalOperator.AND
    assert filters[1].kind == "leaf"
    assert filters[1].cmp_operator == ComparisonOperator.NEQ


def test_vertex_config_no_filters_for_unknown_vertex(vertex_config_with_filters):
    from graflo.architecture.vertex import VertexConfig

    vc = VertexConfig.model_validate(vertex_config_with_filters)
    assert vc.filters("nonexistent") == []


def test_vertex_filter_integration(vertex_config_with_filters, sample_vertex_docs):
    """End-to-end: YAML -> VertexConfig -> filter docs (mirrors actor._filter_and_aggregate_vertex_docs)."""
    from graflo.architecture.vertex import VertexConfig

    vc = VertexConfig.model_validate(vertex_config_with_filters)
    result = _apply_vertex_filters(vc, "feature", sample_vertex_docs)

    kept_names_values = [(d["name"], d["value"]) for d in result]
    assert ("Open", 5.0) in kept_names_values
    assert ("Close", 3.0) in kept_names_values
    assert ("High", -3.0) in kept_names_values
    assert ("Open", -1.0) not in kept_names_values
    assert ("Close", -2.0) not in kept_names_values
    assert ("Volume", 100) not in kept_names_values
    assert len(result) == 3


def test_vertex_filter_no_filters_passes_all(sample_vertex_docs):
    """A vertex with no filters keeps all documents."""
    from graflo.architecture.vertex import VertexConfig

    vc = VertexConfig.model_validate(
        {"vertices": [{"name": "raw", "fields": ["name", "value"]}]}
    )
    result = _apply_vertex_filters(vc, "raw", sample_vertex_docs)
    assert len(result) == len(sample_vertex_docs)
