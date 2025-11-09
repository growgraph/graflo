from typing import Dict, Type

from .onto import (
    ArangoConnectionConfig,
    ConnectionConfig,
    ConnectionKind,
    Neo4jConnectionConfig,
    TigergraphConnectionConfig,
    WSGIConfig,
)

# Define this mapping in a separate file to avoid circular imports
CONNECTION_TYPE_MAPPING: Dict[ConnectionKind, Type[ConnectionConfig]] = {
    ConnectionKind.ARANGO: ArangoConnectionConfig,
    ConnectionKind.NEO4J: Neo4jConnectionConfig,
    ConnectionKind.TIGERGRAPH: TigergraphConnectionConfig,
    ConnectionKind.WSGI: WSGIConfig,
}
