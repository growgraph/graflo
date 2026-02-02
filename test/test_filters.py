import pytest

from graflo.filter.onto import (
    Clause,
    ComparisonOperator,
    Expression,
    LogicalOperator,
)


@pytest.fixture()
def eq_clause():
    # doc.x == 1
    return ["==", "1", "x"]


@pytest.fixture()
def none_clause():
    # doc.x == null
    return ["==", None, "x"]


@pytest.fixture()
def cong_clause():
    # doc.x % 2 == 2
    return ["==", 2, "y", "% 2"]


@pytest.fixture()
def in_clause():
    return [ComparisonOperator.IN, [1, 2]]


@pytest.fixture()
def and_clause(eq_clause, cong_clause):
    return {LogicalOperator.AND: [eq_clause, cong_clause]}


def test_none_leaf(none_clause):
    lc = Clause.from_list(none_clause)
    result = lc()
    assert isinstance(result, str)
    assert "null" in result


def test_leaf_clause_construct(eq_clause):
    lc = Clause.from_list(eq_clause)
    assert lc.cmp_operator == ComparisonOperator.EQ
    assert lc() == 'doc["x"] == "1"'


def test_leaf_clause_construct_(eq_clause):
    lc = Expression.from_dict(eq_clause)
    assert lc.cmp_operator == ComparisonOperator.EQ
    assert lc() == 'doc["x"] == "1"'


def test_init_filter_and(and_clause):
    c = Expression.from_dict(and_clause)
    assert c.operator == LogicalOperator.AND
    assert c() == 'doc["x"] == "1" AND doc["y"] % 2 == 2'


def test_init_filter_eq(eq_clause):
    c = Expression.from_dict(eq_clause)
    assert c() == 'doc["x"] == "1"'


def test_init_filter_in(in_clause):
    c = Expression.from_dict(in_clause)
    assert c() == "IN [1, 2]"
