"""Architecture façade.

For lighter imports, prefer:

- ``graflo.architecture.schema`` — graph schema types
- ``graflo.architecture.contract`` — manifest, bindings, resources, transforms
- ``graflo.architecture.graph_types`` — runtime containers, contexts, indexes (or submodules)
- ``graflo.architecture.pipeline.runtime`` — actors and executor

See ``docs/guides/importing.md`` in the package.

The façade is lazy (PEP 562) so that importing any ``graflo.architecture.X``
submodule does not eagerly load the rest of the architecture tree.
"""

from __future__ import annotations

from typing import Any

_EXPORTS: dict[str, str] = {
    "GraFloIndex": "graflo.architecture.backend",
    "CoreSchema": "graflo.architecture.schema",
    "EdgeConfigDBAware": "graflo.architecture.schema",
    "GraFloOutput": "graflo.architecture.schema",
    "GraphMetadata": "graflo.architecture.schema",
    "GraphModel": "graflo.architecture.schema",
    "Schema": "graflo.architecture.schema",
    "SchemaDBAware": "graflo.architecture.schema",
    "VertexConfigDBAware": "graflo.architecture.schema",
    "Edge": "graflo.architecture.schema.edge",
    "EdgeConfig": "graflo.architecture.schema.edge",
    "FieldType": "graflo.architecture.schema.vertex",
    "Vertex": "graflo.architecture.schema.vertex",
    "VertexConfig": "graflo.architecture.schema.vertex",
    "APIConnector": "graflo.architecture.contract",
    "Bindings": "graflo.architecture.contract",
    "BoundSourceKind": "graflo.architecture.contract",
    "FileConnector": "graflo.architecture.contract",
    "GraphManifest": "graflo.architecture.contract",
    "IngestionModel": "graflo.architecture.contract",
    "KafkaConnector": "graflo.architecture.contract",
    "ProtoTransform": "graflo.architecture.contract",
    "Resource": "graflo.architecture.contract",
    "ResourceConnector": "graflo.architecture.contract",
    "SparqlConnector": "graflo.architecture.contract",
    "TableConnector": "graflo.architecture.contract",
    "Transform": "graflo.architecture.contract",
    "DatabaseProfile": "graflo.architecture.schema.database_features",
    "Index": "graflo.architecture.graph_types",
}

__all__ = [
    "APIConnector",
    "Bindings",
    "BoundSourceKind",
    "CoreSchema",
    "DatabaseProfile",
    "Edge",
    "EdgeConfig",
    "EdgeConfigDBAware",
    "FieldType",
    "FileConnector",
    "GraFloIndex",
    "GraFloOutput",
    "GraphManifest",
    "GraphMetadata",
    "GraphModel",
    "Index",
    "IngestionModel",
    "KafkaConnector",
    "ProtoTransform",
    "Resource",
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


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        import importlib

        value = getattr(importlib.import_module(_EXPORTS[name]), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
