"""Architecture façade.

For lighter imports, prefer:

- ``graflo.architecture.schema`` — graph schema types
- ``graflo.architecture.contract`` — manifest, bindings, resources, transforms
- ``graflo.architecture.graph_types`` — runtime containers, contexts, indexes (or submodules)
- ``graflo.architecture.pipeline.runtime`` — actors and executor

See ``docs/importing.md`` in the package.
"""

from .contract import (
    APIConnector,
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
from graflo.architecture.backend import GraFloIndex
from graflo.architecture.schema import (
    CoreSchema,
    EdgeConfigDBAware,
    GraFloOutput,
    GraphMetadata,
    GraphModel,
    Schema,
    SchemaDBAware,
    VertexConfigDBAware,
)
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import FieldType, Vertex, VertexConfig

_LAZY_EXPORTS = {
    "GraFloBackendConfig": ("graflo.db.graflo_backend.config", "GraFloBackendConfig"),
}


def __getattr__(name: str):
    if name in _LAZY_EXPORTS:
        module_name, attr_name = _LAZY_EXPORTS[name]
        import importlib

        module = importlib.import_module(module_name)
        return getattr(module, attr_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "APIConnector",
    "Bindings",
    "CoreSchema",
    "DatabaseProfile",
    "Edge",
    "EdgeConfig",
    "EdgeConfigDBAware",
    "FieldType",
    "FileConnector",
    "GraFloOutput",
    "GraFloBackendConfig",
    "GraFloIndex",
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
