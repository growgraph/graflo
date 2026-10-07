"""Order resources so that each runs after the resources it depends on."""

from __future__ import annotations

from collections.abc import Iterable, Sequence


def order_resources(
    declared: Sequence[str], constraints: Iterable[tuple[str, str]]
) -> tuple[list[str], list[tuple[str, str]]]:
    """The declared order, moved only as far as *constraints* require.

    Each constraint ``(before, after)`` asks that *before* run first. A stable
    topological sort over the strongly connected components of the
    constraints: at each step the first component, in declared order of its
    earliest member, whose predecessors outside it are all placed comes next,
    its members in declared order. Resources no constraint names keep their
    relative order. A name *declared* does not list is ignored, as is a
    resource constrained against itself.

    Returns:
        The order, and the constraints it leaves unsatisfied. Those are empty
        unless the constraints form a cycle; then only the resources in the
        cycle keep their declared order among themselves, and each constraint
        between two of them that this order violates is returned, in declared
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
    successors: dict[str, list[str]] = {name: [] for name in names}
    for before, after in pairs:
        successors[before].append(after)

    component = _components(names, successors, position)
    members: dict[int, list[str]] = {}
    for name in names:
        members.setdefault(component[name], []).append(name)
    predecessors: dict[int, set[int]] = {c: set() for c in members}
    for before, after in pairs:
        if component[before] != component[after]:
            predecessors[component[after]].add(component[before])

    # ``members`` holds the components in declared order of their earliest
    # member: the order a tie among ready components is broken in.
    order: list[str] = []
    placed: set[int] = set()
    while len(placed) < len(members):
        ready = next(
            c for c in members if c not in placed and predecessors[c] <= placed
        )
        order.extend(members[ready])
        placed.add(ready)
    return order, [
        (before, after)
        for before, after in pairs
        if component[before] == component[after] and position[before] > position[after]
    ]


def _components(
    names: list[str], successors: dict[str, list[str]], position: dict[str, int]
) -> dict[str, int]:
    """Each name's strongly connected component, as its earliest member's position.

    Tarjan's algorithm, iterative so a long constraint chain cannot exhaust
    the recursion limit.
    """
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    component: dict[str, int] = {}
    for root in names:
        if root in index:
            continue
        work: list[tuple[str, int]] = [(root, 0)]
        while work:
            node, next_child = work.pop()
            if next_child == 0:
                index[node] = low[node] = len(index)
                stack.append(node)
                on_stack.add(node)
            children = successors[node]
            if next_child < len(children):
                work.append((node, next_child + 1))
                child = children[next_child]
                if child not in index:
                    work.append((child, 0))
                elif child in on_stack:
                    low[node] = min(low[node], index[child])
                continue
            if low[node] == index[node]:
                scc: list[str] = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    scc.append(member)
                    if member == node:
                        break
                rank = min(position[member] for member in scc)
                for member in scc:
                    component[member] = rank
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
    return component


__all__ = ["order_resources"]
