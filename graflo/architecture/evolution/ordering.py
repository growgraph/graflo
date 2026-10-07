"""Order resources so that each runs after the resources it depends on."""

from __future__ import annotations

from collections.abc import Iterable, Sequence


def order_resources(
    declared: Sequence[str], constraints: Iterable[tuple[str, str]]
) -> tuple[list[str], list[tuple[str, str]]]:
    """The declared order, moved only as far as *constraints* require.

    Each constraint ``(before, after)`` asks that *before* run first. A stable
    topological sort: at each step the first resource in *declared* order
    whose predecessors are all placed comes next, so resources no constraint
    names keep their relative order. A name *declared* does not list is
    ignored, as is a resource constrained against itself.

    Returns:
        The order, and the constraints it leaves unsatisfied. Those are empty
        unless the constraints form a cycle; then the declared order is
        returned unchanged, with every constraint it violates, in declared
        order of their ``after`` resource.
    """
    names = list(declared)
    position = {name: i for i, name in enumerate(names)}
    pairs = sorted(
        {
            (before, after)
            for before, after in constraints
            if before in position and after in position and before != after
        },
        key=lambda pair: (position[pair[1]], position[pair[0]]),
    )
    predecessors: dict[str, set[str]] = {name: set() for name in names}
    for before, after in pairs:
        predecessors[after].add(before)

    order: list[str] = []
    placed: set[str] = set()
    while len(order) < len(names):
        ready = next(
            (
                name
                for name in names
                if name not in placed and predecessors[name] <= placed
            ),
            None,
        )
        if ready is None:
            return names, [
                (before, after)
                for before, after in pairs
                if position[before] > position[after]
            ]
        order.append(ready)
        placed.add(ready)
    return order, []


__all__ = ["order_resources"]
