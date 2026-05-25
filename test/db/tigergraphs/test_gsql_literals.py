"""Tests for GSQL DEFAULT literal formatting."""

from __future__ import annotations

import pytest

from graflo.db.tigergraph.gsql_literals import gsql_default_literal


def test_gsql_default_literal_bool() -> None:
    assert gsql_default_literal(True) == "true"
    assert gsql_default_literal(False) == "false"


def test_gsql_default_literal_numbers() -> None:
    assert gsql_default_literal(0) == "0"
    assert gsql_default_literal(-1) == "-1"
    assert gsql_default_literal(-1.0) == "-1.0"


def test_gsql_default_literal_string() -> None:
    assert gsql_default_literal("a") == '"a"'
    assert gsql_default_literal('say "hi"') == '"say \\"hi\\""'


def test_gsql_default_literal_rejects_nan() -> None:
    with pytest.raises(ValueError, match="NaN"):
        gsql_default_literal(float("nan"))


def test_gsql_default_literal_rejects_unsupported() -> None:
    with pytest.raises(TypeError):
        gsql_default_literal([1])
