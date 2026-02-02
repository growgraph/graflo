import pytest
import yaml

from graflo.filter.onto import Clause, Expression, LogicalOperator
from graflo.onto import ExpressionFlavor


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
    lc = Clause(**clause_open)  # kind=leaf inferred from operator (str)
    doc = {"name": "Open"}
    assert lc(**doc, kind=ExpressionFlavor.PYTHON)


def test_condition_b(clause_b):
    m = Clause(**clause_b)  # kind=leaf inferred from operator (str)
    doc = {"value": -1}
    assert m(value=1, kind=ExpressionFlavor.PYTHON)
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)


def test_clause_a(clause_a):
    m = Expression.from_dict(clause_a)

    doc = {"name": "Open", "value": 5.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Open", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)


def test_clause_ab(clause_ab):
    m = Expression.from_dict(clause_ab)

    doc = {"name": "Open", "value": 5.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Open", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Close", "value": 5.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Close", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)


def test_filter_implication(filter_implication):
    m = Expression.from_dict(filter_implication)

    doc = {"name": "Open", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Close", "value": -1.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)


def test_filter_neq(clause_volume):
    m = Expression.from_dict(clause_volume)

    doc = {"name": "Open", "value": -1.0}
    assert m(kind=ExpressionFlavor.PYTHON, **doc)

    doc = {"name": "Volume", "value": -1.0}
    assert not m(kind=ExpressionFlavor.PYTHON, **doc)
