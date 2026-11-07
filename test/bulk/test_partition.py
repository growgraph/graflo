"""Mix and batch: every cell once, and a round's lanes share no endpoint bucket."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from graflo.hq.bulk.partition import rounds
from test.hypothesis_budget import examples


@settings(max_examples=examples(100))
@given(n=st.integers(min_value=1, max_value=16), same_type=st.booleans())
def test_rounds_cover_every_cell_once_and_never_share_a_bucket(
    n: int, same_type: bool
) -> None:
    schedule = rounds(n, same_type=same_type)

    cells = [cell for lanes in schedule for lane in lanes for cell in lane]
    assert sorted(cells) == [(a, b) for a in range(n) for b in range(n)]
    for lanes in schedule:
        if same_type:
            # One vertex space: a lane touches every bucket of its cells.
            touched = [{x for cell in lane for x in cell} for lane in lanes]
            assert sum(len(t) for t in touched) == len(set().union(*touched))
        else:
            sources = [a for lane in lanes for a, _ in lane]
            targets = [b for lane in lanes for _, b in lane]
            assert len(set(sources)) == len(sources)
            assert len(set(targets)) == len(targets)


def test_a_round_keeps_every_writer_busy() -> None:
    assert [len(lanes) for lanes in rounds(4, same_type=False)] == [4, 4, 4, 4]
    assert [len(lanes) for lanes in rounds(4, same_type=True)] == [4, 2, 2, 2]
