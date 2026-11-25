"""Performance and load cases for Grafeo; run only with ``--run-performance``."""

import pytest

from test.db.cypher_cases.performance import (
    BatchSizingCases,
    ConcurrentLoadCases,
    GraphOperationsLoadCases,
    LimitsCases,
    ScalabilityCases,
    SustainedLoadCases,
    ThroughputCases,
)

pytestmark = pytest.mark.performance


class TestThroughput(ThroughputCases):
    pass


class TestScalability(ScalabilityCases):
    pass


class TestConcurrency(ConcurrentLoadCases):
    pass


class TestBatchSizing(BatchSizingCases):
    pass


class TestSustainedLoad(SustainedLoadCases):
    pass


class TestLimits(LimitsCases):
    pass


class TestGraphOperationsLoad(GraphOperationsLoadCases):
    pass
