"""Edge cases for the Grafeo connector (bodies in ``test.db.cypher_cases``)."""

from test.db.cypher_cases.boundaries import (
    BoundaryConditionsCases,
    BoundaryValueAnalysisCases,
    MalformedInputsCases,
    NumericStringCases,
    PathologicalIdsCases,
    TypeConfusionCases,
)
from test.db.cypher_cases.encoding import (
    EmojiCases,
    LabelAbuseCases,
    MalformedEncodingCases,
    PropertyKeySmugglingCases,
)
from test.db.cypher_cases.graphs import (
    GraphAlgorithmCases,
    LockOrderingCases,
    PathologicalGraphsCases,
    StateCorruptionCases,
)
from test.db.cypher_cases.injection import (
    CypherInjectionCases,
    QueryInjectionAdvancedCases,
    ReDoSCases,
)
from test.db.cypher_cases.queries import (
    AggregationCases,
    EdgeOperationsCases,
    FilterCases,
    FilterStressCases,
    QueryComplexityCases,
    SchemaEvolutionCases,
    TemporalAnomaliesCases,
)
from test.db.cypher_cases.stress import (
    BatchStressCases,
    CartesianLoadCases,
    ConcurrencyCases,
    ConnectionChurnCases,
    MemoryExhaustionCases,
)


class TestBoundaryConditions(BoundaryConditionsCases):
    pass


class TestBoundaryValues(BoundaryValueAnalysisCases):
    pass


class TestMalformedInputs(MalformedInputsCases):
    pass


class TestNumericStrings(NumericStringCases):
    pass


class TestPathologicalIds(PathologicalIdsCases):
    pass


class TestTypeConfusion(TypeConfusionCases):
    pass


class TestEmoji(EmojiCases):
    pass


class TestLabelAbuse(LabelAbuseCases):
    pass


class TestMalformedEncoding(MalformedEncodingCases):
    pass


class TestPropertyKeySmuggling(PropertyKeySmugglingCases):
    pass


class TestGraphAlgorithms(GraphAlgorithmCases):
    pass


class TestLockOrdering(LockOrderingCases):
    pass


class TestPathologicalGraphs(PathologicalGraphsCases):
    pass


class TestStateCorruption(StateCorruptionCases):
    pass


class TestCypherInjection(CypherInjectionCases):
    pass


class TestQueryInjectionAdvanced(QueryInjectionAdvancedCases):
    pass


class TestReDoS(ReDoSCases):
    pass


class TestAggregation(AggregationCases):
    pass


class TestEdgeOperations(EdgeOperationsCases):
    pass


class TestFilters(FilterCases):
    pass


class TestFilterStress(FilterStressCases):
    pass


class TestQueryComplexity(QueryComplexityCases):
    pass


class TestSchemaEvolution(SchemaEvolutionCases):
    pass


class TestTemporalAnomalies(TemporalAnomaliesCases):
    pass


class TestBatchStress(BatchStressCases):
    pass


class TestCartesianLoad(CartesianLoadCases):
    pass


class TestConcurrency(ConcurrencyCases):
    pass


class TestConnectionChurn(ConnectionChurnCases):
    pass


class TestMemoryExhaustion(MemoryExhaustionCases):
    pass
