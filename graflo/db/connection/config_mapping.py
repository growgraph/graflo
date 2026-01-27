from typing import Dict, Type

from .onto import (
    ArangoConfig,
    DBConfig,
    FalkordbConfig,
    MemgraphConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    TigergraphConfig,
)
from ... import DBType

# Define this mapping in a separate file to avoid circular imports
DB_TYPE_MAPPING: Dict[DBType, Type[DBConfig]] = {
    DBType.ARANGO: ArangoConfig,
    DBType.NEO4J: Neo4jConfig,
    DBType.TIGERGRAPH: TigergraphConfig,
    DBType.FALKORDB: FalkordbConfig,
    DBType.MEMGRAPH: MemgraphConfig,
    DBType.NEBULA: NebulaConfig,
    DBType.POSTGRES: PostgresConfig,
}


def get_config_class(db_type: DBType) -> Type[DBConfig]:
    """Get the appropriate config class for a database type.

    This factory function breaks the circular dependency by moving the
    lookup logic out of the DBType enum.

    Args:
        db_type: The database type enum value

    Returns:
        The corresponding DBConfig subclass

    Raises:
        KeyError: If the db_type is not in the mapping
    """
    return DB_TYPE_MAPPING[db_type]
