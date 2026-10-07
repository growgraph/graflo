"""Resource order: the declared order, moved only as far as constraints require."""

from __future__ import annotations

from graflo.architecture.evolution.ordering import order_resources


def test_no_constraints_keep_the_declared_order() -> None:
    assert order_resources(["c", "a", "b"], []) == (["c", "a", "b"], [])


def test_a_constrained_resource_waits_for_its_predecessor() -> None:
    """``b`` waits for ``d``; ``c``, free to go, keeps its place before ``d``."""
    order, cycles = order_resources(["a", "b", "c", "d"], [("d", "b")])
    assert order == ["a", "c", "d", "b"]
    assert cycles == []


def test_a_satisfied_constraint_changes_nothing() -> None:
    assert order_resources(["a", "b", "c"], [("a", "c")]) == (["a", "b", "c"], [])


def test_unconstrained_resources_keep_their_relative_order() -> None:
    order, _ = order_resources(["x", "b", "y", "a", "z"], [("a", "b")])
    assert order == ["x", "y", "a", "b", "z"]


def test_chained_constraints_are_followed() -> None:
    order, _ = order_resources(["c", "b", "a"], [("a", "b"), ("b", "c")])
    assert order == ["a", "b", "c"]


def test_names_outside_the_declared_list_are_ignored() -> None:
    assert order_resources(["a", "b"], [("ghost", "a"), ("b", "nowhere")]) == (
        ["a", "b"],
        [],
    )


def test_a_cycle_keeps_the_declared_order_and_returns_what_it_breaks() -> None:
    order, cycles = order_resources(
        ["a", "b", "c"], [("a", "b"), ("b", "a"), ("a", "c")]
    )
    assert order == ["a", "b", "c"]
    assert cycles == [("b", "a")]


def test_a_cycle_keeps_the_declared_order_only_among_its_resources() -> None:
    """``a``/``b`` form a cycle; ``d`` still runs before ``c``."""
    order, cycles = order_resources(
        ["a", "b", "c", "d"], [("a", "b"), ("b", "a"), ("d", "c")]
    )
    assert order == ["a", "b", "d", "c"]
    assert cycles == [("b", "a")]


def test_a_cycle_waits_for_its_predecessors_outside_it() -> None:
    """The cycle ``a``/``b``/``c`` depends on ``d``, which depends on ``e``."""
    order, cycles = order_resources(
        ["a", "b", "c", "d", "e"],
        [("a", "b"), ("b", "c"), ("c", "a"), ("e", "d"), ("d", "b")],
    )
    assert order == ["e", "d", "a", "b", "c"]
    assert cycles == [("c", "a")]


def test_a_long_constraint_chain_is_ordered() -> None:
    names = [f"r{i}" for i in range(3000)]
    order, cycles = order_resources(names, list(zip(names[1:], names[:-1])))
    assert order == names[::-1]
    assert cycles == []
