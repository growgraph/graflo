from graflo.architecture.graph_types import (
    LocationIndex,
)


def test_lindex():
    la = LocationIndex(("a", "b", "c"))
    lb = LocationIndex(("a", "b", "c"))
    lc = LocationIndex(("a", "b", "d"))
    ld = LocationIndex((0,))
    assert la.congruence_measure(lb) == 3
    assert la.congruence_measure(lc) == 2
    assert la.congruence_measure(ld) == 0
