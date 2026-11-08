"""Rounds of edge cells that share no endpoint vertex (mix and batch).

The fold hashes each edge's source and target keys into ``n`` buckets, which
puts every edge of a run in one cell ``(source bucket, target bucket)``. Cells
whose buckets differ touch disjoint vertices, so their edges can be created in
parallel without two writers waiting on one node's lock. A round is a list of
lanes, each a few cells one writer takes in order; the lanes of a round run in
parallel and rounds run one after another.

When the source and target types are the same, a vertex's bucket is the same in
either role, so a cell touches both of its buckets: a lane then holds the two
cells of one bucket pair and a round's pairs are a matching.
"""

from __future__ import annotations

Cell = tuple[int, int]
Lane = tuple[Cell, ...]


def rounds(n: int, *, same_type: bool) -> list[list[Lane]]:
    """Every cell of the ``n`` by ``n`` grid, once, in rounds of disjoint lanes.

    Args:
        n: Buckets per endpoint.
        same_type: Source and target are one vertex type.
    """
    if n < 1:
        raise ValueError(f"need at least one bucket, got {n}")
    if not same_type:
        # Round i pairs source bucket (i + j) mod n with target bucket j.
        return [[(((i + j) % n, j),) for j in range(n)] for i in range(n)]
    out: list[list[Lane]] = [[((a, a),) for a in range(n)]]
    # The circle method: bucket 0 stays, the others rotate; with an odd n a
    # phantom bucket n sits out one pair per round.
    m = n + n % 2
    others = list(range(1, m))
    for r in range(m - 1):
        ring = [0, *others[r:], *others[:r]]
        lanes: list[Lane] = []
        for k in range(m // 2):
            a, b = ring[k], ring[m - 1 - k]
            if a < n and b < n:
                lanes.append(((a, b), (b, a)))
        if lanes:
            out.append(lanes)
    return out
