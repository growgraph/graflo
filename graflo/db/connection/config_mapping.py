from typing import Dict, Type

from .onto import (
    ArangoConfig,
    DBConfig,
    DBType,
    Neo4jConfig,
    PostgresConfig,
    TigergraphConfig,
)

# Define this mapping in a separate file to avoid circular imports
DB_TYPE_MAPPING: Dict[DBType, Type[DBConfig]] = {
    DBType.ARANGO: ArangoConfig,
    DBType.NEO4J: Neo4jConfig,
    DBType.TIGERGRAPH: TigergraphConfig,
    DBType.POSTGRES: PostgresConfig,
}
