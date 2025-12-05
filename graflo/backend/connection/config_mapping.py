from typing import Dict, Type

from .onto import (
    ArangoConfig,
    BackendType,
    DBConfig,
    Neo4jConfig,
    TigergraphConfig,
)

# Define this mapping in a separate file to avoid circular imports
BACKEND_TYPE_MAPPING: Dict[BackendType, Type[DBConfig]] = {
    BackendType.ARANGO: ArangoConfig,
    BackendType.NEO4J: Neo4jConfig,
    BackendType.TIGERGRAPH: TigergraphConfig,
}
