"""Tests for Transform.planned_output_field_names."""

from __future__ import annotations

from graflo.architecture.contract.ingestion.transform import (
    DressConfig,
    KeySelectionConfig,
    Transform,
)


def test_planned_output_field_names_from_output() -> None:
    t = Transform(
        module="builtins",
        foo="int",
        input=("a",),
        output=("age",),
    )
    assert t.planned_output_field_names() == ("age",)


def test_planned_output_field_names_from_dress() -> None:
    t = Transform(
        module="builtins",
        foo="str",
        input=("Open",),
        dress=DressConfig(key="name", value="value"),
    )
    assert t.planned_output_field_names() == ("name", "value")


def test_planned_output_field_names_from_rename() -> None:
    t = Transform(rename={"src": "dst"})
    assert t.planned_output_field_names() == ("dst",)


def test_planned_output_field_names_from_output_groups() -> None:
    t = Transform(
        module="builtins",
        foo="int",
        input_groups=(("a", "b"),),
        output_groups=(("x", "y"),),
    )
    assert t.planned_output_field_names() == ("x", "y")


def test_planned_output_field_names_target_keys() -> None:
    t = Transform(
        module="builtins",
        foo="str",
        target="keys",
        keys=KeySelectionConfig(mode="include", names=("a", "c")),
    )
    assert t.planned_output_field_names({"a": 1, "b": 2, "c": 3}) == ("a", "c")
