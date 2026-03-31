"""Architecture façade.

For lighter imports, prefer:

- ``graflo.architecture.schema`` — graph schema types
- ``graflo.architecture.contract`` — manifest, bindings, resources, transforms
- ``graflo.architecture.pipeline.runtime`` — actors and executor

See ``docs/importing.md`` in the package.
"""

from .contract import (
    Bindings,
    FileConnector,
    GraphManifest,
    IngestionModel,
    JoinClause,
    ProtoTransform,
    Resource,
    BoundSourceKind,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
    Transform,
)
from .database_features import DatabaseProfile
from .graph_types import Index
from graflo.architecture.schema import (
    CoreSchema,
    EdgeConfigDBAware,
    GraphMetadata,
    GraphModel,
    Schema,
    SchemaDBAware,
    VertexConfigDBAware,
)
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import FieldType, Vertex, VertexConfig

__all__ = [
    "Bindings",
    "CoreSchema",
    "DatabaseProfile",
    "Edge",
    "EdgeConfig",
    "EdgeConfigDBAware",
    "FieldType",
    "FileConnector",
    "GraphManifest",
    "GraphMetadata",
    "GraphModel",
    "Index",
    "IngestionModel",
    "JoinClause",
    "ProtoTransform",
    "Resource",
    "BoundSourceKind",
    "ResourceConnector",
    "Schema",
    "SchemaDBAware",
    "SparqlConnector",
    "TableConnector",
    "Transform",
    "Vertex",
    "VertexConfig",
    "VertexConfigDBAware",
]
