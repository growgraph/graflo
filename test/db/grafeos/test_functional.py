"""Functional cases for the Grafeo connector (bodies in ``test.db.cypher_cases``)."""

from test.db.cypher_cases.functional import (
    CommonMistakesCases,
    DataCoherenceCases,
    DataQualityCases,
    ETLScenariosCases,
    FunctionalQueryCases,
    FunctionalSchemaEvolutionCases,
    MultiSourceSyncCases,
    OperationConsistencyCases,
    PerformancePitfallCases,
    PolymorphicRelationshipsCases,
    RelationshipsCases,
    TransactionalRobustnessCases,
    TraversalCases,
)


class TestDataCoherence(DataCoherenceCases):
    pass


class TestMultiSourceSync(MultiSourceSyncCases):
    pass


class TestRelationships(RelationshipsCases):
    pass


class TestPolymorphicRelationships(PolymorphicRelationshipsCases):
    pass


class TestETLScenarios(ETLScenariosCases):
    pass


class TestSchemaEvolution(FunctionalSchemaEvolutionCases):
    pass


class TestQueries(FunctionalQueryCases):
    pass


class TestTraversal(TraversalCases):
    pass


class TestTransactionalRobustness(TransactionalRobustnessCases):
    pass


class TestPerformancePitfalls(PerformancePitfallCases):
    pass


class TestCommonMistakes(CommonMistakesCases):
    pass


class TestDataQuality(DataQualityCases):
    pass


class TestOperationConsistency(OperationConsistencyCases):
    pass
