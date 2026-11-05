from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.onto import DBType

from .onto import (
    ArangoConfig,
    DBConfig,
    FalkordbConfig,
    GraphDBConfig,
    MemgraphConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    SparqlEndpointConfig,
    TigergraphConfig,
)

# Define this mapping in a separate file to avoid circular imports
DB_TYPE_MAPPING: dict[DBType, type[DBConfig]] = {
    DBType.ARANGO: ArangoConfig,
    DBType.NEO4J: Neo4jConfig,
    DBType.TIGERGRAPH: TigergraphConfig,
    DBType.FALKORDB: FalkordbConfig,
    DBType.MEMGRAPH: MemgraphConfig,
    DBType.NEBULA: NebulaConfig,
    DBType.POSTGRES: PostgresConfig,
    DBType.SPARQL: SparqlEndpointConfig,
    DBType.GRAPHDB: GraphDBConfig,
    DBType.GRAFLO_BACKEND: GraFloBackendConfig,
}


def get_config_class(db_type: DBType) -> type[DBConfig]:
    """Get the appropriate config class for a database type.

    This factory function breaks the circular dependency by moving the
    lookup logic out of the DBType enum.

    Args:
        db_type: The database type enum value

    Returns:
        The corresponding DBConfig subclass

    Raises:
        ValueError: If no config class serves *db_type*.
    """
    config_class = DB_TYPE_MAPPING.get(db_type)
    if config_class is None:
        raise ValueError(f"no connection config serves database type {db_type.value!r}")
    return config_class
