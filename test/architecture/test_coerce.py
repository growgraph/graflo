"""coerce_value: a raw value made to conform to its field's declared type."""

from __future__ import annotations

import uuid
from datetime import date, datetime

import pytest

from graflo.architecture.schema.coerce import CoercionError, coerce_value
from graflo.architecture.schema.semantics import FieldSemantics
from graflo.architecture.schema.vertex import Field


def _field(type_: str | None, *, item_type: str | None = None, unit: str | None = None):
    return Field(
        name="f",
        type=type_,
        item_type=item_type,
        semantics=FieldSemantics(unit=unit) if unit else None,
    )


@pytest.mark.parametrize(
    ("type_", "raw", "expected"),
    [
        ("int", 3, 3),
        ("int", "42", 42),
        ("int", " -7 ", -7),
        ("int", 3.0, 3),
        ("int", "1e3", 1000),
        ("uint", "5", 5),
        ("float", "2.5", 2.5),
        ("double", 4, 4.0),
        ("float", "-1.5e-3", -0.0015),
        ("bool", True, True),
        ("bool", "Yes", True),
        ("bool", "FALSE", False),
        ("bool", 0, False),
        ("string", "  Ann Lee ", "Ann Lee"),
        ("string", 12, "12"),
        ("datetime", "2024-03-01", "2024-03-01"),
        ("datetime", "2024-03-01 10:00", "2024-03-01T10:00"),
        ("datetime", "2024-03-01T10:00:05.5z", "2024-03-01T10:00:05.5Z"),
        ("datetime", "2024-03-01T10:00:05+02:00", "2024-03-01T10:00:05+02:00"),
        ("datetime", date(2024, 3, 1), "2024-03-01"),
        ("datetime", datetime(2024, 3, 1, 9, 30), "2024-03-01T09:30:00"),
    ],
)
def test_accepts(type_, raw, expected):
    assert coerce_value(raw, _field(type_)) == expected


@pytest.mark.parametrize("raw", ["2024", "2024-03"])
def test_partial_dates_are_kept_as_written(raw):
    assert coerce_value(raw, _field("datetime")) == raw


@pytest.mark.parametrize(
    ("type_", "raw"),
    [
        ("int", True),
        ("int", "1,000"),
        ("int", 2.5),
        ("int", "abc"),
        ("uint", -1),
        ("uint", "-3"),
        ("float", "nan"),
        ("float", float("inf")),
        ("float", False),
        ("float", "1_000"),
        ("bool", "maybe"),
        ("bool", 2),
        ("string", True),
        ("datetime", "March 2024"),
        ("datetime", "2024-13"),
        ("datetime", "2024-02-30"),
        ("datetime", 2024),
        ("uuid", "not-a-uuid"),
        ("int", {"a": 1}),
        ("string", ["a"]),
    ],
)
def test_refuses_with_kind_type(type_, raw):
    with pytest.raises(CoercionError) as info:
        coerce_value(raw, _field(type_))
    assert info.value.kind == "type"
    assert info.value.field == "f"
    assert info.value.value == raw


@pytest.mark.parametrize("raw", [None, "", "   ", []])
def test_empty_values_are_refused(raw):
    with pytest.raises(CoercionError) as info:
        coerce_value(raw, _field("string"))
    assert info.value.kind == "empty"


def test_uuid_is_lower_cased():
    value = str(uuid.uuid4())
    assert coerce_value(value.upper(), _field("uuid")) == value


def test_untyped_field_passes_the_value_through():
    payload = {"nested": [1, 2]}
    assert coerce_value(payload, _field(None)) is payload


def test_list_coerces_each_item_and_wraps_a_scalar():
    field = _field("list", item_type="int")
    assert coerce_value(["1", 2, 3.0], field) == [1, 2, 3]
    assert coerce_value("7", field) == [7]


def test_list_reports_the_failing_item_index():
    with pytest.raises(CoercionError) as info:
        coerce_value(["1", "x", "3"], _field("list", item_type="int"))
    assert info.value.kind == "list_item"
    assert info.value.index == 1


@pytest.mark.parametrize(
    ("raw", "expected"), [("200 kV", 200.0), ("200kV", 200.0), ("-1.5 kV", -1.5)]
)
def test_declared_unit_is_stripped(raw, expected):
    assert coerce_value(raw, _field("float", unit="kV")) == expected


def test_declared_unit_is_stripped_from_list_items():
    field = _field("list", item_type="int", unit="m")
    assert coerce_value(["3 m", "4"], field) == [3, 4]


@pytest.mark.parametrize(
    ("raw", "unit"), [("0.2 MV", "kV"), ("200 kv", "kV"), ("5 kg", None)]
)
def test_other_units_are_a_unit_mismatch(raw, unit):
    with pytest.raises(CoercionError) as info:
        coerce_value(raw, _field("float", unit=unit))
    assert info.value.kind == "unit_mismatch"


def test_unit_is_not_stripped_from_strings():
    assert coerce_value("200 kV", _field("string", unit="kV")) == "200 kV"
